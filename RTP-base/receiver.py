import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3
HEADER_SIZE = 16


def make_ack(seq_num):
    header = PacketHeader(type=ACK, seq_num=seq_num, length=0, checksum=0)
    header.checksum = compute_checksum(header)
    return bytes(header)


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


def receiver(receiver_ip, receiver_port, window_size):
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((receiver_ip, receiver_port))
    active = False
    sender_address = None
    expected_seq = 1
    buffered = {}

    try:
        while True:
            raw, address = sock.recvfrom(2048)
            parsed = parse_packet(raw)
            if parsed is None:
                continue

            packet_type, seq_num, payload = parsed
            if not active:
                if packet_type != START or seq_num != 0 or payload:
                    continue
                active = True
                sender_address = address
                sock.sendto(make_ack(1), sender_address)
                continue

            if address != sender_address or packet_type == START:
                continue

            if packet_type == DATA:
                if seq_num < expected_seq:
                    sock.sendto(make_ack(expected_seq), sender_address)
                    continue
                if seq_num >= expected_seq + window_size:
                    continue

                buffered.setdefault(seq_num, payload)
                while expected_seq in buffered:
                    sys.stdout.buffer.write(buffered.pop(expected_seq))
                    expected_seq += 1
                sys.stdout.buffer.flush()
                sock.sendto(make_ack(expected_seq), sender_address)
                continue

            if packet_type == END and not payload and seq_num == expected_seq:
                sock.sendto(make_ack(expected_seq + 1), sender_address)
                return
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()
    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
