"""RTP sender with selective acknowledgements and one window timer."""

import argparse
import os
import socket
import sys
import time

from utils import PacketHeader, compute_checksum


START, END, DATA, ACK = range(4)
HEADER_SIZE = 16
PAYLOAD_SIZE = 1472 - HEADER_SIZE
RETRY_INTERVAL = 0.5


def make_packet(kind, sequence, payload=b""):
    header = PacketHeader(type=kind, seq_num=sequence, length=len(payload))
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def read_ack(channel, destination, deadline=None):
    """Discard invalid ACKs without extending an absolute deadline."""
    while True:
        if deadline is None:
            channel.settimeout(None)
        else:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            channel.settimeout(remaining)
        try:
            wire, source = channel.recvfrom(65535)
        except socket.timeout:
            return None
        if source != destination or len(wire) != HEADER_SIZE:
            continue
        header = PacketHeader(wire)
        if header.type != ACK or header.length != 0:
            continue
        received_crc = header.checksum
        header.checksum = 0
        if compute_checksum(header) == received_crc:
            return header.seq_num


def sender(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be positive")
    if os.name == "nt":
        import msvcrt

        msvcrt.setmode(sys.stdin.fileno(), os.O_BINARY)
    source = sys.stdin.buffer
    destination = (socket.gethostbyname(receiver_ip), receiver_port)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
        channel.sendto(make_packet(START, 0), destination)
        # The assignment excludes loss/delay of the START acknowledgement.
        while read_ack(channel, destination) != 1:
            pass

        left_edge = 1
        next_sequence = 1
        unacknowledged = {}
        exhausted = False
        deadline = None

        while unacknowledged or not exhausted:
            # ACKs above a missing packet do not open the right edge yet.
            while not exhausted and next_sequence < left_edge + window_size:
                payload = source.read(PAYLOAD_SIZE)
                if not payload:
                    exhausted = True
                    break
                wire = make_packet(DATA, next_sequence, payload)
                unacknowledged[next_sequence] = wire
                channel.sendto(wire, destination)
                next_sequence += 1

            if not unacknowledged:
                break
            if deadline is None:
                deadline = time.monotonic() + RETRY_INTERVAL

            sequence = read_ack(channel, destination, deadline)
            if sequence is None:
                # One timer, retransmitting only packets without individual ACKs.
                for wire in unacknowledged.values():
                    channel.sendto(wire, destination)
                deadline = time.monotonic() + RETRY_INTERVAL
            elif sequence in unacknowledged:
                del unacknowledged[sequence]
                previous_edge = left_edge
                while left_edge < next_sequence and left_edge not in unacknowledged:
                    left_edge += 1
                if left_edge != previous_edge:
                    deadline = None

        channel.sendto(make_packet(END, next_sequence), destination)
        deadline = time.monotonic() + RETRY_INTERVAL
        while True:
            acknowledgement = read_ack(channel, destination, deadline)
            if acknowledgement is None or acknowledgement == next_sequence + 1:
                break


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receiver_ip", help="Receiver IPv4 address or host name")
    parser.add_argument("receiver_port", type=int, help="Receiver UDP port")
    parser.add_argument("window_size", type=int, help="Maximum unacknowledged packets")
    args = parser.parse_args()
    if not 1 <= args.receiver_port <= 65535:
        parser.error("receiver_port must be between 1 and 65535")
    if args.window_size < 1:
        parser.error("window_size must be positive")
    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
