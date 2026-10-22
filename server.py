# distributed md5 cracker - server side
import argparse
import hashlib
import json
import socketserver
import threading
import time

# the md5 we need to crack (string is a 10 digit number)
DEFAULT_TARGET = "ec9c0f7edcc18a98b1f31853b1813301"

NUMBER_LENGTH = 10
MAX_NUMBER = 10 ** NUMBER_LENGTH      # all 10 digit numbers
DEFAULT_BLOCK_SIZE = 1_000_000


class SharedState:
    # all the worker connections touch this, so lock it
    def __init__(self, target, block_size, max_number):
        self.target = target.lower()
        self.block_size = block_size
        self.max_number = max_number

        self.lock = threading.Lock()
        self.next_start = 0           # next number not handed out yet
        self.start_time = None        # when the first block went out

        self.solution = None
        self.finder_machine = None
        self.found_time = None
        self.solution_event = threading.Event()

        self.machines = {}            # machine_id -> join time (computers)
        self.workers = set()          # unique worker ids (cores)
        self.exhausted = False

    def register(self, machine_id, worker_id):
        # remember who joined so we can count and print
        new_machine = False
        with self.lock:
            if machine_id not in self.machines:
                self.machines[machine_id] = time.time()
                new_machine = True
            self.workers.add(worker_id)
            machine_count = len(self.machines)
        if new_machine:
            print(f"[server] new computer joined: {machine_id}  "
                  f"(computers so far: {machine_count})", flush=True)

    def next_block(self):
        # give out the next block, or None if we're done
        with self.lock:
            if self.solution_event.is_set():
                return None
            if self.next_start >= self.max_number:
                self.exhausted = True
                return None
            if self.start_time is None:
                self.start_time = time.time()
            start = self.next_start
            size = min(self.block_size, self.max_number - start)
            self.next_start += size
            return start, size

    def report_found(self, machine_id, candidate):
        # dont just trust the client - check the md5 ourselves
        digest = hashlib.md5(candidate.encode()).hexdigest().lower()
        if digest != self.target:
            print(f"[server] got a wrong solution from {machine_id} "
                  f"('{candidate}') - rejected", flush=True)
            return False
        with self.lock:
            if not self.solution_event.is_set():
                self.solution = candidate
                self.finder_machine = machine_id
                self.found_time = time.time()
                self.solution_event.set()
        return True


class WorkerHandler(socketserver.StreamRequestHandler):
    # one connection = one core. it keeps asking for blocks on the same socket.
    # messages are one json per line
    def handle(self):
        state = self.server.state
        try:
            for raw in self.rfile:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                mtype = msg.get("type")
                if mtype == "get_block":
                    state.register(msg["machine_id"], msg["worker_id"])
                    block = state.next_block()
                    if block is None:
                        self._send({"type": "stop"})
                        break
                    start, size = block
                    self._send({"type": "block", "start": start, "size": size})
                elif mtype == "found":
                    ok = state.report_found(msg["machine_id"], msg["candidate"])
                    self._send({"type": "found_ack", "correct": ok})
                    if ok:
                        break
        except (ConnectionError, OSError):
            pass

    def _send(self, obj):
        self.wfile.write((json.dumps(obj) + "\n").encode("utf-8"))
        self.wfile.flush()


class ComputeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, state):
        super().__init__(addr, WorkerHandler)
        self.state = state


def print_results(state):
    # everything the exercise asks us to print when we find it
    elapsed = state.found_time - (state.start_time or state.found_time)
    print("\n" + "=" * 50, flush=True)
    print("  FOUND IT", flush=True)
    print("=" * 50, flush=True)
    print(f"  original string : {state.solution}", flush=True)
    print(f"  md5             : {state.target}", flush=True)
    print(f"  computers used  : {len(state.machines)}", flush=True)
    print(f"  cores used      : {len(state.workers)}", flush=True)
    print(f"  time to find    : {elapsed:.2f} sec", flush=True)
    print(f"  found by        : {state.finder_machine}", flush=True)
    print("=" * 50 + "\n", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9009)
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    parser.add_argument("--max", type=int, default=MAX_NUMBER)  # smaller for testing
    args = parser.parse_args()

    state = SharedState(args.target, args.block_size, args.max)
    server = ComputeServer((args.host, args.port), state)

    print(f"[server] listening on {args.host}:{args.port}", flush=True)
    print(f"[server] cracking md5: {state.target}", flush=True)
    print(f"[server] range 0..{args.max:,}  block size {args.block_size:,}",
          flush=True)
    print("[server] waiting for computers...\n", flush=True)

    # run the server in the background so the main thread can wait for the answer
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()

    try:
        while not state.solution_event.wait(timeout=1.0):
            with state.lock:
                exhausted = state.exhausted
            if exhausted:
                # whole range handed out, give the last blocks a sec to finish
                if state.solution_event.wait(timeout=5.0):
                    break
                print("[server] checked everything, no solution found", flush=True)
                server.shutdown()
                return
        print_results(state)
    except KeyboardInterrupt:
        print("\n[server] stopped", flush=True)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
