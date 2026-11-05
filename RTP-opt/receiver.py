"""RTP receiver with individual DATA ACKs and ordered binary output."""

import argparse
import os
import socket
import sys

from utils import PacketHeader, compute_checksum


START, END, DATA, ACK = range(4)
HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472


def unpack_packet(wire):
    if not HEADER_SIZE <= len(wire) <= MAX_PACKET_SIZE:
        return None
    header = PacketHeader(wire[:HEADER_SIZE])
    payload = wire[HEADER_SIZE:]
    if header.type not in (START, END, DATA) or header.length != len(payload):
        return None
    if header.type != DATA and payload:
        return None
    received_crc = header.checksum
    header.checksum = 0
    if compute_checksum(header / payload) != received_crc:
        return None
    return header.type, header.seq_num, payload


def acknowledge(channel, destination, sequence):
    header = PacketHeader(type=ACK, seq_num=sequence, length=0)
    header.checksum = compute_checksum(header)
    channel.sendto(bytes(header), destination)


def receiver(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be positive")
    if os.name == "nt":
        import msvcrt

        msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
    sink = sys.stdout.buffer

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
        channel.bind((receiver_ip, receiver_port))
        peer = None
        expected = 1
        buffered = {}

        while True:
            wire, address = channel.recvfrom(65535)
            if peer is not None and address != peer:
                continue
            packet = unpack_packet(wire)
            if packet is None:
                continue
            kind, sequence, payload = packet

            if peer is None:
                if kind == START and sequence == 0:
                    peer = address
                    acknowledge(channel, peer, 1)
                continue

            if kind == DATA:
                if sequence < 1 or sequence >= expected + window_size:
                    continue
                if sequence >= expected:
                    buffered.setdefault(sequence, payload)
                wrote_data = False
                while expected in buffered:
                    sink.write(buffered.pop(expected))
                    expected += 1
                    wrote_data = True
                if wrote_data:
                    sink.flush()
                # Re-ACK old DATA too: a previous ACK may have been lost.
                acknowledge(channel, peer, sequence)
            elif kind == END and sequence == expected and not buffered:
                acknowledge(channel, peer, sequence + 1)
                sink.flush()
                return
            # Ignore START while a connection is already active.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receiver_ip", help="Local IPv4 address or host name")
    parser.add_argument("receiver_port", type=int, help="Local UDP port")
    parser.add_argument("window_size", type=int, help="Receive window in packets")
    args = parser.parse_args()
    if not 1 <= args.receiver_port <= 65535:
        parser.error("receiver_port must be between 1 and 65535")
    if args.window_size < 1:
        parser.error("window_size must be positive")
    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
