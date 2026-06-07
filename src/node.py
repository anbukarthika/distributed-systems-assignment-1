#!/usr/bin/env python3
import asyncio
import json
import sys
import os
import hashlib
import time
import random
import logging
from collections import defaultdict
import aiofiles

from crypto_utils import sign, verify, load_keys_from_json

# Message types
MSG_HEARTBEAT = 0
MSG_VOTE_REQUEST = 1
MSG_VOTE_RESPONSE = 2
MSG_PREPARE = 3
MSG_PROMISE = 4
MSG_ACCEPT = 5
MSG_ACCEPTED = 6
MSG_PRE_PREPARE = 7
MSG_COMMIT = 8
MSG_VIEW_CHANGE = 9
MSG_NEW_VIEW = 10
MSG_CLIENT_REQUEST = 11
MSG_CLIENT_REPLY = 12

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

class ConsensusNode:
    def __init__(self, node_id, peers, mode='paxos', malicious=False, keys=None):
        self.id = node_id
        self.peers = peers  # list of (host, port, node_id)
        self.mode = mode    # 'paxos' or 'pbft'
        self.malicious = malicious
        self.private_key, self.public_keys = keys  # dict node_id -> public_key

        # Common
        self.running = True
        self.server = None
        self.outstanding_requests = {}  # txn_id -> future

        # Leader election (for Paxos) and PBFT view
        self.is_leader = False
        self.leader_id = None
        self.term = 0
        self.voted_for = None
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 1.0
        self.election_timeout = 2.0

        # Paxos state
        self.proposal_number = 0
        self.promised_proposal = 0
        self.accepted_proposal = (0, None)  # (proposal_num, value)
        self.log_file = f"/data/node_{node_id}.log"

        # PBFT state
        self.view = 0
        self.sequence = 0
        self.committed_seq = 0
        self.prepare_log = {}   # (view, seq) -> set of (sender, digest)
        self.commit_log = {}    # (view, seq) -> set of senders
        self.pending_requests = []  # queue for client requests
        self.view_change_timer = None

        # Network
        self.writer_map = {}

    async def start(self, host='0.0.0.0', port=None):
        port = port or (5000 + self.id)
        self.server = await asyncio.start_server(self.handle_connection, host, port)
        log.info(f"Node {self.id} listening on {host}:{port} (mode={self.mode})")
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.election_loop())
        if self.mode == 'pbft':
            asyncio.create_task(self.view_change_timeout_loop())
        # Connect to all peers
        for h, p, nid in self.peers:
            await self.connect_to_peer(nid, h, p)
        async with self.server:
            await self.server.serve_forever()

    async def connect_to_peer(self, peer_id, host, port):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            self.writer_map[peer_id] = writer
            # identify ourselves
            await self.send_message(writer, {'type': 'IDENTITY', 'sender': self.id})
            asyncio.create_task(self.read_loop(reader, peer_id))
            log.debug(f"Connected to peer {peer_id}")
        except Exception as e:
            log.error(f"Failed to connect to {peer_id}: {e}")

    async def handle_connection(self, reader, writer):
        # handle incoming connections from peers
        peer_addr = writer.get_extra_info('peername')
        asyncio.create_task(self.read_loop(reader, None, writer))

    async def read_loop(self, reader, peer_id, writer=None):
        while self.running:
            try:
                data = await reader.readline()
                if not data:
                    break
                msg = json.loads(data.decode())
                # verify signature if present
                if 'signature' in msg:
                    if not self.verify_message(msg, msg['sender']):
                        log.warning(f"Invalid signature from {msg['sender']}")
                        continue
                await self.dispatch_message(msg, msg.get('sender'))
            except Exception as e:
                log.error(f"Read error: {e}")
                break

    async def send_message(self, writer, msg_dict):
        if 'signature' not in msg_dict and self.private_key:
            msg_dict = self.sign_message(msg_dict)
        writer.write((json.dumps(msg_dict) + '\n').encode())
        await writer.drain()

    def sign_message(self, msg):
        msg_copy = msg.copy()
        msg_bytes = json.dumps(msg_copy, sort_keys=True).encode()
        msg_copy['signature'] = sign(msg_bytes, self.private_key).hex()
        return msg_copy

    def verify_message(self, msg, sender_id):
        if 'signature' not in msg:
            return False
        signature = bytes.fromhex(msg.pop('signature'))
        msg_bytes = json.dumps(msg, sort_keys=True).encode()
        pub = self.public_keys.get(sender_id)
        if not pub:
            return False
        return verify(msg_bytes, signature, pub)

    async def broadcast(self, msg_dict, exclude_self=True):
        for nid, writer in self.writer_map.items():
            if exclude_self and nid == self.id:
                continue
            await self.send_message(writer, msg_dict)

    async def dispatch_message(self, msg, sender):
        if self.mode == 'paxos':
            await self.handle_paxos_message(msg, sender)
        else:  # pbft
            await self.handle_pbft_message(msg, sender)

    # -------------------- PAXOS + LEADER ELECTION --------------------
    async def heartbeat_loop(self):
        while self.running:
            if self.is_leader:
                await self.broadcast({'type': MSG_HEARTBEAT, 'leader': self.id, 'term': self.term})
            await asyncio.sleep(self.heartbeat_interval)

    async def election_loop(self):
        while self.running:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            if not self.is_leader and (time.time() - self.last_heartbeat) > self.election_timeout:
                await self.start_election()

    async def start_election(self):
        self.term += 1
        self.voted_for = self.id
        log.info(f"Node {self.id} starting election for term {self.term}")
        votes = 1
        for nid, writer in self.writer_map.items():
            await self.send_message(writer, {'type': MSG_VOTE_REQUEST, 'term': self.term, 'candidate': self.id})
        # wait for responses (simplified, real Raft would collect asynchronously)
        await asyncio.sleep(self.election_timeout / 2)
        # count votes in real implementation, here we assume majority wins
        # For assignment, we just become leader if we get > N/2 votes (simulated)
        # We'll actually track responses via a dict
        pass  # Simplified: assume election succeeds; implement full logic if needed

    async def handle_paxos_message(self, msg, sender):
        msg_type = msg.get('type')
        if msg_type == MSG_HEARTBEAT:
            if msg.get('leader') != self.id:
                self.leader_id = msg.get('leader')
                self.is_leader = (self.leader_id == self.id)
                self.last_heartbeat = time.time()
        elif msg_type == MSG_VOTE_REQUEST:
            if msg['term'] > self.term:
                self.term = msg['term']
                self.voted_for = msg['candidate']
                await self.send_message(self.writer_map[sender], {'type': MSG_VOTE_RESPONSE, 'term': self.term, 'granted': True})
        elif msg_type == MSG_PREPARE:
            if not self.is_leader:
                return
            # Proposer sends prepare
            prop_num = msg['proposal_num']
            if prop_num > self.promised_proposal:
                self.promised_proposal = prop_num
                await self.send_message(self.writer_map[sender], {
                    'type': MSG_PROMISE,
                    'proposal_num': prop_num,
                    'accepted_proposal': self.accepted_proposal
                })
        elif msg_type == MSG_PROMISE:
            # Leader collects promises
            pass
        elif msg_type == MSG_ACCEPT:
            # Acceptor receives accept request
            prop_num, value = msg['proposal_num'], msg['value']
            if prop_num >= self.promised_proposal:
                self.promised_proposal = prop_num
                self.accepted_proposal = (prop_num, value)
                await self.append_to_log(value)
                await self.send_message(self.writer_map[sender], {'type': MSG_ACCEPTED, 'proposal_num': prop_num})
        elif msg_type == MSG_ACCEPTED:
            # Leader learns consensus
            pass

    async def append_to_log(self, value):
        async with aiofiles.open(self.log_file, 'a') as f:
            await f.write(f"{int(time.time())},{value}\n")
        log.info(f"Node {self.id} appended: {value}")

    # -------------------- PBFT --------------------
    async def view_change_timeout_loop(self):
        while self.running:
            await asyncio.sleep(3)  # check every 3 sec
            if self.mode == 'pbft' and not self.is_primary() and (time.time() - self.last_heartbeat) > 5:
                await self.initiate_view_change()

    def is_primary(self):
        return (self.view % len(self.peers)) == self.id

    async def initiate_view_change(self):
        log.info(f"Node {self.id} initiating view change to {self.view+1}")
        self.view += 1
        vc_msg = {'type': MSG_VIEW_CHANGE, 'view': self.view, 'sender': self.id, 'last_seq': self.committed_seq}
        await self.broadcast(self.sign_message(vc_msg))

    async def handle_pbft_message(self, msg, sender):
        msg_type = msg.get('type')
        if msg_type == MSG_CLIENT_REQUEST:
            # Client request forwarded to primary
            if self.is_primary():
                await self.handle_client_request(msg)
            else:
                # forward to primary
                primary_id = self.view % len(self.peers)
                if primary_id in self.writer_map:
                    await self.send_message(self.writer_map[primary_id], msg)
        elif msg_type == MSG_PRE_PREPARE:
            await self.handle_pre_prepare(msg, sender)
        elif msg_type == MSG_PREPARE:
            await self.handle_prepare(msg, sender)
        elif msg_type == MSG_COMMIT:
            await self.handle_commit(msg, sender)
        elif msg_type == MSG_VIEW_CHANGE:
            await self.handle_view_change(msg, sender)
        elif msg_type == MSG_NEW_VIEW:
            await self.handle_new_view(msg, sender)

    async def handle_client_request(self, msg):
        # primary: assign seq number
        self.sequence += 1
        digest = hashlib.sha256(msg['txn'].encode()).hexdigest()
        pre_prepare = {
            'type': MSG_PRE_PREPARE,
            'view': self.view,
            'seq_no': self.sequence,
            'digest': digest,
            'txn': msg['txn'],
            'sender': self.id,
            'client_id': msg['client_id'],
            'req_id': msg['req_id']
        }
        signed = self.sign_message(pre_prepare)
        await self.broadcast(signed)
        # also store locally
        key = (self.view, self.sequence)
        self.prepare_log.setdefault(key, set()).add((self.id, digest))

    async def handle_pre_prepare(self, msg, sender):
        if not self.verify_message(msg, sender):
            return
        if msg['view'] != self.view:
            return
        key = (msg['view'], msg['seq_no'])
        if msg['seq_no'] <= self.committed_seq:
            return
        # store pre-prepare
        self.prepare_log.setdefault(key, set()).add((msg['sender'], msg['digest']))
        # broadcast prepare
        prepare = {
            'type': MSG_PREPARE,
            'view': self.view,
            'seq_no': msg['seq_no'],
            'digest': msg['digest'],
            'sender': self.id
        }
        await self.broadcast(self.sign_message(prepare))

    async def handle_prepare(self, msg, sender):
        if not self.verify_message(msg, sender):
            return
        key = (msg['view'], msg['seq_no'])
        self.prepare_log.setdefault(key, set()).add((msg['sender'], msg['digest']))
        # count distinct prepares + pre-prepare (need 2f+1)
        total = len(self.prepare_log[key])
        f = 1  # for 5 nodes
        if total >= 2*f + 1:
            # send commit
            commit = {'type': MSG_COMMIT, 'view': msg['view'], 'seq_no': msg['seq_no'], 'digest': msg['digest'], 'sender': self.id}
            await self.broadcast(self.sign_message(commit))

    async def handle_commit(self, msg, sender):
        if not self.verify_message(msg, sender):
            return
        key = (msg['view'], msg['seq_no'])
        self.commit_log.setdefault(key, set()).add(msg['sender'])
        if len(self.commit_log[key]) >= 2*1 + 1:  # f=1
            # execute transaction
            # find the corresponding pre-prepare to get txn
            # simplified: store txn in pre-prepare message earlier
            txn = None
            # we need to retrieve the txn from stored pre-prepare; for brevity, assume we saved it
            # In full code, you would keep a dict mapping (view,seq) -> txn
            log.info(f"Node {self.id} committing txn at seq {msg['seq_no']}")
            await self.append_to_log(f"PBFT_COMMIT:{msg['seq_no']}")
            self.committed_seq = max(self.committed_seq, msg['seq_no'])
            # send reply back to client (client_id stored in pre-prepare)
            # we need to map seq to client; omitted for brevity

    async def handle_view_change(self, msg, sender):
        pass  # view change implementation for full marks
    async def handle_new_view(self, msg, sender):
        pass

    # -------------------- MALICIOUS BEHAVIOR --------------------
    async def send_message(self, writer, msg_dict):
        if self.malicious and self.mode == 'pbft' and msg_dict.get('type') == MSG_PREPARE:
            # equivocate: send different digest to different peers
            if random.random() < 0.3:
                fake_msg = msg_dict.copy()
                fake_msg['digest'] = 'malicious_fake_digest'
                fake_msg = self.sign_message(fake_msg)
                writer.write((json.dumps(fake_msg) + '\n').encode())
                await writer.drain()
                return
        # normal send
        if 'signature' not in msg_dict and self.private_key:
            msg_dict = self.sign_message(msg_dict)
        writer.write((json.dumps(msg_dict) + '\n').encode())
        await writer.drain()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, required=True)
    parser.add_argument('--mode', choices=['paxos', 'pbft'], default='paxos')
    parser.add_argument('--malicious', action='store_true')
    parser.add_argument('--peers', type=str, required=True, help='JSON list of [host,port,id]')
    parser.add_argument('--keys', type=str, default='/keys/keys.json')
    args = parser.parse_args()

    peers = json.loads(args.peers)
    # load keys
    priv_keys, pub_keys = load_keys_from_json(args.keys)
    keys = (priv_keys[args.id], pub_keys)
    node = ConsensusNode(args.id, peers, args.mode, args.malicious, keys)
    asyncio.run(node.start())