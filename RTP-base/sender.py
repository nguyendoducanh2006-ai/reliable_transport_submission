import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16
MAX_PAYLOAD_SIZE = 1472 - HEADER_SIZE
RETRANSMIT_TIMEOUT = 0.5


def sender(receiver_ip, receiver_port, window_size):
    """Open socket and send message from sys.stdin."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    msg = sys.stdin.buffer.read()

    # Split message into chunks
    chunks = []
    for offset in range(0, len(msg), MAX_PAYLOAD_SIZE):
        chunks.append(msg[offset : offset + MAX_PAYLOAD_SIZE])

    # Build all DATA packets (seq_num starts from 1)
    packets = []
    for i, chunk in enumerate(chunks):
        pkt_header = PacketHeader(type=2, seq_num=i + 1, length=len(chunk), checksum=0)
        pkt_header.checksum = compute_checksum(pkt_header / chunk)
        pkt = bytes(pkt_header / chunk)
        packets.append(pkt)

    # Send START packet (seq_num=0) and wait for ACK (seq_num=1)
    pkt_header = PacketHeader(type=0, seq_num=0, length=0, checksum=0)
    pkt_header.checksum = compute_checksum(pkt_header)
    start_pkt = bytes(pkt_header)
    while True:
        s.sendto(start_pkt, (receiver_ip, receiver_port))
        readable, _, _ = select.select([s], [], [], RETRANSMIT_TIMEOUT)
        if readable:
            data, address = s.recvfrom(2048)
            if len(data) >= HEADER_SIZE:
                pkt_header = PacketHeader(data[:HEADER_SIZE])
                pkt_checksum = pkt_header.checksum
                pkt_header.checksum = 0
                computed_checksum = compute_checksum(pkt_header)
                if pkt_checksum == computed_checksum and pkt_header.type == 3 and pkt_header.seq_num == 1:
                    break

    # Sliding window: cumulative ACK (Go-Back-N)
    base = 1
    next_seq = 1
    last_seq = len(packets)
    window_sent = {}
    timer_started = None

    while base <= last_seq:
        # Send packets within the window
        while next_seq <= last_seq and next_seq < base + window_size:
            pkt = packets[next_seq - 1]
            s.sendto(pkt, (receiver_ip, receiver_port))
            window_sent[next_seq] = pkt
            next_seq += 1
            if timer_started is None:
                timer_started = time.monotonic()

        # Wait for ACK or timeout
        remaining = RETRANSMIT_TIMEOUT - (time.monotonic() - timer_started)
        readable, _, _ = select.select([s], [], [], max(0, remaining))
        if readable:
            data, address = s.recvfrom(2048)
            if len(data) >= HEADER_SIZE:
                pkt_header = PacketHeader(data[:HEADER_SIZE])
                pkt_checksum = pkt_header.checksum
                pkt_header.checksum = 0
                computed_checksum = compute_checksum(pkt_header)
                if pkt_checksum == computed_checksum and pkt_header.type == 3:
                    ack_seq = pkt_header.seq_num
                    if base < ack_seq <= next_seq:
                        for seq in range(base, ack_seq):
                            window_sent.pop(seq, None)
                        base = ack_seq
                        timer_started = time.monotonic() if base <= last_seq else None
            continue

        # Timeout: retransmit all packets in the window
        for pkt in window_sent.values():
            s.sendto(pkt, (receiver_ip, receiver_port))
        timer_started = time.monotonic()

    # Send END packet and wait for ACK (up to 500ms)
    end_seq = last_seq + 1
    pkt_header = PacketHeader(type=1, seq_num=end_seq, length=0, checksum=0)
    pkt_header.checksum = compute_checksum(pkt_header)
    end_pkt = bytes(pkt_header)
    s.sendto(end_pkt, (receiver_ip, receiver_port))
    readable, _, _ = select.select([s], [], [], RETRANSMIT_TIMEOUT)
    if readable:
        s.recvfrom(2048)

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
