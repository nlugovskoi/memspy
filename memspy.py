#!/usr/bin/env python3

import argparse
import datetime
import os
import psutil
import subprocess
import sys
import threading
import time
import queue

from dataclasses import dataclass
from typing import List, Union


@dataclass
class ProcessInfo:
    pid: int
    mem: int
    cmd: List[str]


@dataclass
class ProcessTreeInfo:
    pi: ProcessInfo
    children: List[ProcessInfo]


class ProcessTreeInfoPrinter(object):
    def __init__(self, pt: ProcessTreeInfo):
        self.__pt = pt

    def produce_txt(self) -> str:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return "prob> [{ts}] pid={pid}, mem={mem:.3f}M :\n{children}".format(
            ts=timestamp,
            pid=self.__pt.pi.pid,
            mem=self.__pt.pi.mem / (1024*1024),
            children=''.join([
                f"prob> [{timestamp}]   - pid={c.pid}, mem={(c.mem / (1024*1024)):.3f}M, cmd={c.cmd}\n" for c in self.__pt.children
            ])
        )


def get_process_tree_info(pid: int) -> Union[ProcessTreeInfo, None]:
    try:
        process = psutil.Process(pid)
        memory_info = process.memory_info()
    except psutil.NoSuchProcess:  # cannot find process, usually means it's done
        return None
    except Exception as e:  # something else happened
        raise e

    root_info = ProcessInfo(
        pid=pid,
        mem=memory_info.rss,
        cmd=[]
    )

    process_tree_info = ProcessTreeInfo(root_info, list())

    try:
        children = process.children(recursive=True)
    except Exception:  # no children or process is dead
        return process_tree_info

    for child in children:
        try:
            process_tree_info.children.append(
                ProcessInfo(
                    pid=child.pid,
                    mem=child.memory_info().rss,
                    cmd=child.cmdline()
                )
            )
        except Exception:  # some children might be already done
            continue

    return process_tree_info


class StreamReader(object):
    def __init__(self, stream, line_prefix: str):
        self.__q = queue.Queue()
        self.__line_prefix = line_prefix
        threading.Thread(target=StreamReader.__worker, args=(stream, self.__q), daemon=True).start()

    @staticmethod
    def __worker(stream, q):
        stream.flush()
        for line in iter(stream.readline, ''):
            q.put(line)

    def __read_line(self) -> Union[str, None]:
        while True:
            try:
                line = self.__q.get_nowait()
            except queue.Empty:
                return None
            except Exception:
                return None
            else:
                if isinstance(line, str):
                    yield line.rstrip()
                else:
                    return None

    def read_lines(self) -> List[str]:
        return [f"{self.__line_prefix}> {line}" for line in self.__read_line() if line is not None]


def try_remove_file(file: str) -> bool:
    try:
        os.remove(file)
    except FileNotFoundError:
        pass
    except PermissionError:
        print(f"Error: Permission denied: unable to rewrite '{file}'")
        return False
    except Exception as e:
        print(f"Error: unable to rewrite '{file}': {e}")
        return False

    return True


def monitor_command(interval: int, output_file: str, command: List[str]):
    try_remove_file(output_file) or sys.exit(1)

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    except Exception as e:
        print(f"Error: Cannot start process: {e}")
        sys.exit(1)

    try:
        stdout_reader = StreamReader(process.stdout, "stdout")
        stderr_reader = StreamReader(process.stderr, "stderr")
    except Exception as e:
        print(f"Error: Cannot setup process stdout/stderr reader: {e}")
        sys.exit(1)

    while process.poll() is None:
        try:
            process_tree_info = get_process_tree_info(process.pid)
        except Exception as e:
            print(f"Error: Cannot get process (pid={process.pid}) info: {e}")
            sys.exit(1)

        if process_tree_info is None:  # we are done
            return

        log_lines = [ProcessTreeInfoPrinter(process_tree_info).produce_txt()]

        log_lines.extend(stdout_reader.read_lines())
        log_lines.extend(stderr_reader.read_lines())

        log_lines = [f"{line}\n" for line in log_lines]

        with open(output_file, 'a') as f:
            f.writelines(log_lines)
            f.flush()

        sys.stdout.writelines(log_lines)
        sys.stdout.flush()

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description='A memory probing program that operates at regular intervals ')
    parser.add_argument('-i', '--interval', type=int, required=True, help='An interval in seconds')
    parser.add_argument('-o', '--output', required=True, type=str, help='Output file')
    parser.add_argument('command', nargs=argparse.REMAINDER, help='Command with arguments')

    args = parser.parse_args()

    monitor_command(args.interval, args.output, args.command)


if __name__ == "__main__":
    main()
