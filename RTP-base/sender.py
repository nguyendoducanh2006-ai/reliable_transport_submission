import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3
HEADER_SIZE = 16
MAX_PAYLOAD_SIZE = 1472 - HEADER_SIZE
RETRANSMIT_TIMEOUT = 0.5


def make_packet(packet_type, seq_num, payload=b""):
    header = PacketHeader(type=packet_type, seq_num=seq_num, length=len(payload), checksum=0)
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def parse_packet(raw):
    if len(raw) < HEADER_SIZE:
        return None
    header = PacketHeader(raw[:HEADER_SIZE])
    if header.length < 0 or len(raw) != HEADER_SIZE + header.length:
        return None
    payload = raw[HEADER_SIZE:]
    checksum = header.checksum
    header.checksum = 0
    if compute_checksum(header / payload) != checksum:
        return None
    return header.type, header.seq_num, payload


def wait_for_ack(sock, expected_seq, timeout):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        readable, _, _ = select.select([sock], [], [], remaining)
        if not readable:
            return False
        parsed = parse_packet(sock.recvfrom(2048)[0])
        if parsed is not None and parsed[0] == ACK and parsed[1] == expected_seq:
            return True


def sender(receiver_ip, receiver_port, window_size):
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    destination = (receiver_ip, receiver_port)
    message = sys.stdin.buffer.read()
    packets = [
        make_packet(DATA, seq_num, message[offset:offset + MAX_PAYLOAD_SIZE])
        for seq_num, offset in enumerate(range(0, len(message), MAX_PAYLOAD_SIZE), start=1)
    ]

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        start_packet = make_packet(START, 0)
        while True:
            sock.sendto(start_packet, destination)
            if wait_for_ack(sock, 1, RETRANSMIT_TIMEOUT):
                break

        base = 1
        next_seq = 1
        last_seq = len(packets)
        window_sent = {}
        timer_started = None

        while base <= last_seq:
            while next_seq <= last_seq and next_seq < base + window_size:
                packet = packets[next_seq - 1]
                sock.sendto(packet, destination)
                window_sent[next_seq] = packet
                next_seq += 1
                if timer_started is None:
                    timer_started = time.monotonic()

            remaining = RETRANSMIT_TIMEOUT - (time.monotonic() - timer_started)
            readable, _, _ = select.select([sock], [], [], max(0, remaining))
            if readable:
                parsed = parse_packet(sock.recvfrom(2048)[0])
                if parsed is None or parsed[0] != ACK:
                    continue
                ack_seq = parsed[1]
                if base < ack_seq <= next_seq:
                    for seq_num in range(base, ack_seq):
                        window_sent.pop(seq_num, None)
                    base = ack_seq
                    timer_started = time.monotonic() if base <= last_seq else None
                continue

            for packet in window_sent.values():
                sock.sendto(packet, destination)
            timer_started = time.monotonic()

        end_seq = last_seq + 1
        end_packet = make_packet(END, end_seq)
        sock.sendto(end_packet, destination)
        wait_for_ack(sock, end_seq + 1, RETRANSMIT_TIMEOUT)
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
