import asyncio
import random
import struct
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Set, Optional

import zmq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
import uvicorn


# =========================
# 配置区
# =========================

BT_HOST = "192.168.2.96"
BT_PORT = 1667

WEB_HOST = "0.0.0.0"
WEB_PORT = 8080

PROTOCOL_ID = 2

# Groot2 protocol request types
REQ_FULLTREE = ord("T")
REQ_STATUS = ord("S")

# ZMQ 超时时间，单位 ms
ZMQ_RECV_TIMEOUT_MS = 5000
ZMQ_SEND_TIMEOUT_MS = 5000

# 状态轮询周期，单位秒
STATUS_POLL_INTERVAL_SEC = 0.05

# 是否打印每个节点状态变化
PRINT_STATUS_DETAIL = True

# 是否保存 XML
SAVE_TREE_XML = True
TREE_XML_PATH = "tree.xml"


STATUS_MAP = {
    0: "IDLE",
    1: "RUNNING",
    2: "SUCCESS",
    3: "FAILURE",
    4: "SKIPPED",

    # BT.CPP Groot2Publisher 中，回到 IDLE 的状态通常编码为 10 + previous_status
    11: "IDLE_FROM_RUNNING",
    12: "IDLE_FROM_SUCCESS",
    13: "IDLE_FROM_FAILURE",
    14: "IDLE_FROM_SKIPPED",
}


app = FastAPI()

viewers: Set[WebSocket] = set()

latest_tree_message: Optional[Dict[str, Any]] = None
latest_status: Dict[int, str] = {}


# =========================
# Groot2 ZMQ 客户端
# =========================

def make_header(req_type: int) -> bytes:
    """
    Groot2 请求头：
        protocol_id:  uint8
        request_type: uint8
        unique_id:    uint32 little-endian

    总长度 6 字节。
    """
    unique_id = random.randint(1, 0xFFFFFFFF)
    return struct.pack("<BBI", PROTOCOL_ID, req_type, unique_id)


class Groot2Client:
    def __init__(self, host: str, port: int):
        self.address = f"tcp://{host}:{port}"
        self.ctx = zmq.Context()

        self.socket = self.ctx.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, ZMQ_RECV_TIMEOUT_MS)
        self.socket.setsockopt(zmq.SNDTIMEO, ZMQ_SEND_TIMEOUT_MS)
        self.socket.setsockopt(zmq.LINGER, 0)

        print(f"[bridge] connecting {self.address}")
        self.socket.connect(self.address)

    def request(self, req_type: int) -> List[bytes]:
        header = make_header(req_type)
        self.socket.send_multipart([header])
        return self.socket.recv_multipart()

    def get_full_tree_xml(self) -> str:
        reply = self.request(REQ_FULLTREE)

        print("[bridge] FULLTREE reply parts:", len(reply))
        for i, part in enumerate(reply):
            print(f"[bridge] FULLTREE part[{i}] size={len(part)} preview={part[:80]!r}")

        if len(reply) < 2:
            raise RuntimeError(f"FULLTREE reply parts invalid: {len(reply)}")

        return reply[1].decode("utf-8", errors="replace")

    def get_status_buffer(self) -> bytes:
        reply = self.request(REQ_STATUS)

        if len(reply) < 2:
            raise RuntimeError(f"STATUS reply parts invalid: {len(reply)}")

        return reply[1]

    def close(self):
        try:
            self.socket.close(0)
        except Exception:
            pass

        try:
            self.ctx.term()
        except Exception:
            pass


# =========================
# XML -> 前端树结构
# =========================

def strip_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[-1]
    return tag


def get_uid_from_element(elem: ET.Element) -> Optional[int]:
    """
    兼容不同 XML 字段名。
    当前你的 XML 使用的是 _uid。
    """
    for key in ("_uid", "UID", "uid", "uid16", "node_uid"):
        if key in elem.attrib:
            value = elem.attrib[key]
            if str(value).isdigit():
                return int(value)

    return None


def get_node_name(elem: ET.Element) -> str:
    """
    name 优先作为显示名；
    ID 通常是节点注册名，也可以作为显示名。
    """
    for key in ("name", "ID", "id"):
        if key in elem.attrib:
            return elem.attrib[key]

    return strip_namespace(elem.tag)


def convert_xml_to_tree(xml_text: str) -> Dict[str, Any]:
    """
    把 Groot2Publisher 返回的 XML 转成前端树结构。

    注意：
    <BehaviorTree> 只是包装节点，不是实际运行节点；
    <TreeNodesModel> 是节点类型声明区，不是运行树。
    所以真正要显示的是 <BehaviorTree> 的子节点，例如 Sequence。
    """
    root = ET.fromstring(xml_text)

    real_uid_count = 0
    generated_uid_count = 0
    uid_counter = 100000

    def walk(elem: ET.Element) -> Optional[Dict[str, Any]]:
        nonlocal real_uid_count, generated_uid_count, uid_counter

        tag = strip_namespace(elem.tag)

        # TreeNodesModel 是节点类型声明，不是运行树
        if tag in ("TreeNodesModel", "include"):
            return None

        uid = get_uid_from_element(elem)

        if uid is None:
            uid = uid_counter
            uid_counter += 1
            generated_uid_count += 1
        else:
            real_uid_count += 1

        node = {
            "uid": uid,
            "name": get_node_name(elem),
            "kind": tag,
            "status": "IDLE",
            "children": [],
        }

        for child in list(elem):
            child_node = walk(child)
            if child_node is not None:
                node["children"].append(child_node)

        return node

    # 找到第一个 BehaviorTree
    behavior_tree = None

    for elem in root:
        if strip_namespace(elem.tag) == "BehaviorTree":
            behavior_tree = elem
            break

    if behavior_tree is None:
        raise RuntimeError("No <BehaviorTree> found in XML")

    # BehaviorTree 本身只是包装，不显示它；显示它的实际根节点
    runtime_children = []

    for child in list(behavior_tree):
        child_node = walk(child)
        if child_node is not None:
            runtime_children.append(child_node)

    if len(runtime_children) == 0:
        raise RuntimeError("<BehaviorTree> has no runtime child nodes")

    if len(runtime_children) == 1:
        tree_root = runtime_children[0]
    else:
        tree_root = {
            "uid": 999999,
            "name": behavior_tree.attrib.get("ID", "BehaviorTree"),
            "kind": "BehaviorTree",
            "status": "IDLE",
            "children": runtime_children,
        }
        generated_uid_count += 1

    print("[bridge] XML parsed.")
    print("[bridge] real uid count:", real_uid_count)
    print("[bridge] generated uid count:", generated_uid_count)
    print("[bridge] runtime root:", tree_root["name"], "uid:", tree_root["uid"])

    return {
        "type": "tree",
        "root": tree_root,
        "raw_xml": xml_text,
    }


# =========================
# STATUS buffer 解析
# =========================

def parse_status_buffer(buf: bytes) -> List[Dict[str, Any]]:
    """
    Groot2 STATUS buffer:
    每个节点 3 字节：
        uint16 node_uid
        uint8  status_code
    """
    updates = []

    if len(buf) % 3 != 0:
        print("[bridge] warning: status buffer size is not multiple of 3:", len(buf))

    for offset in range(0, len(buf), 3):
        if offset + 3 > len(buf):
            break

        uid, status_code = struct.unpack_from("<HB", buf, offset)
        status = STATUS_MAP.get(status_code, f"UNKNOWN_{status_code}")

        updates.append({
            "type": "status",
            "uid": uid,
            "status": status,
            "status_code": status_code,
            "timestamp_ms": int(time.time() * 1000),
        })

    return updates


# =========================
# WebSocket 广播
# =========================

async def broadcast(message: Dict[str, Any]):
    dead_clients = []

    for ws in viewers:
        try:
            await ws.send_json(message)
        except Exception:
            dead_clients.append(ws)

    for ws in dead_clients:
        viewers.discard(ws)


# =========================
# 后台轮询 Groot2Publisher
# =========================

async def groot2_poll_loop():
    global latest_tree_message, latest_status

    while True:
        client = None

        try:
            client = Groot2Client(BT_HOST, BT_PORT)

            # 1. 请求整棵树
            xml_text = client.get_full_tree_xml()

            print("[bridge] tree loaded, xml size:", len(xml_text))

            if SAVE_TREE_XML:
                with open(TREE_XML_PATH, "w", encoding="utf-8") as f:
                    f.write(xml_text)
                print(f"[bridge] tree xml saved to: {TREE_XML_PATH}")

            latest_tree_message = convert_xml_to_tree(xml_text)
            latest_status.clear()

            await broadcast(latest_tree_message)

            # 2. 持续轮询状态
            while True:
                buf = client.get_status_buffer()
                updates = parse_status_buffer(buf)

                changed_count = 0

                for msg in updates:
                    uid = msg["uid"]
                    status = msg["status"]

                    old_status = latest_status.get(uid)

                    if old_status != status:
                        latest_status[uid] = status
                        changed_count += 1

                        if PRINT_STATUS_DETAIL:
                            print("[bridge] status changed:", uid, old_status, "->", status)

                        await broadcast(msg)

                # 只在状态变化时打印，避免刷屏
                if changed_count > 0:
                    print("[bridge] status buffer:", len(buf), "updates:", len(updates))
                    print("[bridge] changed:", changed_count)

                await asyncio.sleep(STATUS_POLL_INTERVAL_SEC)

        except zmq.Again as e:
            print("[bridge] timeout:", repr(e))
            await broadcast({
                "type": "bridge_status",
                "status": "timeout",
                "message": "ZMQ request timeout",
                "timestamp_ms": int(time.time() * 1000),
            })
            await asyncio.sleep(2)

        except Exception as e:
            print("[bridge] error:", repr(e))
            await broadcast({
                "type": "bridge_status",
                "status": "error",
                "message": str(e),
                "timestamp_ms": int(time.time() * 1000),
            })
            await asyncio.sleep(2)

        finally:
            if client is not None:
                client.close()


# =========================
# Web 接口
# =========================

@app.websocket("/ws/viewer")
async def viewer_ws(ws: WebSocket):
    await ws.accept()
    viewers.add(ws)

    print("[web] viewer connected. count:", len(viewers))

    try:
        # 新网页连接后，先发一次当前树
        if latest_tree_message is not None:
            await ws.send_json(latest_tree_message)

        # 再发一次当前所有状态
        for uid, status in latest_status.items():
            await ws.send_json({
                "type": "status",
                "uid": uid,
                "status": status,
                "timestamp_ms": int(time.time() * 1000),
            })

        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        viewers.discard(ws)
        print("[web] viewer disconnected. count:", len(viewers))

    except Exception as e:
        viewers.discard(ws)
        print("[web] viewer error:", repr(e))


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(groot2_poll_loop())


app.mount("/", StaticFiles(directory="web", html=True), name="web")


if __name__ == "__main__":
    uvicorn.run(app, host=WEB_HOST, port=WEB_PORT)