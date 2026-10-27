import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


def receiver(receiver_ip, receiver_port, window_size):
    """TODO: Listen on socket and print received message to sys.stdout."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    active = False
    sender_address = None
    expected_seq = 1
    buffered = {}

    while True:
        # Receive packet; address includes both IP and port
        pkt, address = s.recvfrom(2048)

        # Extract header and payload
        pkt_header = PacketHeader(pkt[:16])
        msg = pkt[16 : 16 + pkt_header.length]

        # Verity checksum
        pkt_checksum = pkt_header.checksum
        pkt_header.checksum = 0
        computed_checksum = compute_checksum(pkt_header / msg)
        if pkt_checksum != computed_checksum:
            continue

        # Wait for START to initiate connection
        if not active:
            if pkt_header.type != 0 or pkt_header.seq_num != 0 or pkt_header.length != 0:
                continue
            active = True
            sender_address = address
            # Send ACK for START packet (seq_num=1)
            ack_header = PacketHeader(type=3, seq_num=1, length=0, checksum=0)
            ack_header.checksum = compute_checksum(ack_header)
            s.sendto(bytes(ack_header), sender_address)
            continue

        # Ignore packets from other senders or duplicate START
        if address != sender_address or pkt_header.type == 0:
            continue

        # Handle DATA packets
        if pkt_header.type == 2:
            seq_num = pkt_header.seq_num

            # Drop packets outside the window
            if seq_num >= expected_seq + window_size:
                continue

            # Duplicate or old packet: send individual ACK
            if seq_num < expected_seq:
                ack_header = PacketHeader(type=3, seq_num=seq_num, length=0, checksum=0)
                ack_header.checksum = compute_checksum(ack_header)
                s.sendto(bytes(ack_header), sender_address)
                continue

            # Buffer in-order or out-of-order packet
            if seq_num not in buffered:
                buffered[seq_num] = msg

            # Deliver in-order packets to stdout
            while expected_seq in buffered:
                sys.stdout.buffer.write(buffered.pop(expected_seq))
                expected_seq += 1
            sys.stdout.buffer.flush()

            # Send individual ACK (seq_num of received packet)
            ack_header = PacketHeader(type=3, seq_num=seq_num, length=0, checksum=0)
            ack_header.checksum = compute_checksum(ack_header)
            s.sendto(bytes(ack_header), sender_address)
            continue

        # Handle END packet
        if pkt_header.type == 1 and pkt_header.length == 0 and pkt_header.seq_num == expected_seq:
            ack_header = PacketHeader(type=3, seq_num=expected_seq + 1, length=0, checksum=0)
            ack_header.checksum = compute_checksum(ack_header)
            s.sendto(bytes(ack_header), sender_address)
            break

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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
