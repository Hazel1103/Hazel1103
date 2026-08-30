
# 🏥 PulseBridge — 院内智能监护 AI Agent + 硬件接口网关

> **Role:** Project Lead & Backend Developer (Intern → Core Contributor)  
> **Company:** 西安元启医疗科技软件有限公司  
> **Duration:** 2025.07 – 2026.03  
> **Stack:** `C++17` · `Python 3.11` · `FastAPI` · `gRPC` · `LangGraph` · `pybind11` · `Redis` · `Kafka`

---

## 📌 Project Background

院内床旁监护设备（心电、血氧、血压等）协议碎片化严重（Modbus / HL7 / DICOM），数据上云延迟高、告警依赖人工阈值、缺乏智能决策闭环。项目目标：构建 **Device ↔ Edge Gateway ↔ Cloud Agent** 全链路，将 AI Agent 的决策能力下沉到床旁，实现实时数据采集 → 异常检测 → 自动告警 → 处置建议的端到端闭环。

**PulseBridge** 取义 *Pulse（脉搏）× Bridge（桥接）*——连接实时硬件信号与灵活 AI 编排，打通从床旁设备到智能决策的最后一公里。

作为**项目主理人**，我主导架构设计、硬件接口引擎开发、后端微服务搭建及 Agent 编排层的集成，推动项目从 0 到 1 落地并部署至 3 家合作医院。

---

## 🏗 Architecture


                                    ┌─────────────────────┐
                                    │   Cloud / Hospital   │
                                    │   AI Agent Layer     │
                                    │  ┌───────────────┐  │
                                    │  │  LangGraph    │  │
                                    │  │  State Machine│  │
                                    │  └──────┬────────┘  │
                                    │         │ Tool Call  │
                                    └─────────┼───────────┘
                                              │
                                   gRPC / WebSocket │
                                              │
┌──────────┐   Modbus/HL7/DICOM   ┌───────────▼──────────┐
│  Bedside │ ◄──────────────────► │   PulseBridge Edge   │
│ Devices  │   (Serial / TCP)     │ ┌───────────────────┐│
│          │                      │ │  C++17 Poller     ││  epoll + io_uring
│ ECG SpO2 │                      │ │  (256 dev/thread) ││  2.3ms/register
│ BP  Temp │                      │ └─────────┬─────────┘│
└──────────┘                      │           │           │
                                  │  ┌────────▼────────┐  │
                                  │  │ pybind11 Bridge │  │  zero-copy
                                  │  │ (NumPy buffer)  │  │  6.4μs/call
                                  │  └────────┬────────┘  │
                                  │           │           │
                                  │  ┌────────▼────────┐  │
                                  │  │ FastAPI Service │  │  12,500 req/s
                                  │  │  asyncio + WS   │  │  5,000 conns
                                  │  └─────────────────┘  │
                                  └───────────────────────┘


---

## 🛠 Tech Stack

| Layer | Technology | Language |
|-------|-----------|----------|
| Hardware Interface | Modbus TCP/RTU, HL7 v2, DICOM, epoll, io_uring | **C++17** |
| Backend Service | FastAPI, asyncio, gRPC, WebSocket, Pydantic | **Python 3.11** |
| Agent Layer | LangGraph, Tool Calling, RAG (FAISS + vLLM) | **Python 3.11** |
| IPC Bridge | pybind11, NumPy buffer protocol (zero-copy) | **C++17 ↔ Python** |
| Message & Cache | Kafka, Redis | — |

---

## 💼 Key Responsibilities

- 作为**项目主理人**，负责整体技术选型、模块拆分、迭代排期与跨组（硬件/算法/前端）协作
- 自研 **PulseBridge C++ 多协议轮询引擎**，统一抽象 Modbus / HL7 / DICOM 设备接入
- 基于 **pybind11** 实现 C++ ↔ Python 零拷贝数据桥接，供 AI Agent 实时消费
- 搭建 **FastAPI 异步微服务集群**，承载设备数据接入、实时推送与 Agent 调度
- 设计 **LangGraph 状态机**，将设备抽象为 `Device as a Tool`，实现 Agent 自主决策闭环
- 编写压测脚本与性能基线，输出 23 篇技术文档

---

## ⚙️ Technical Implementation

### 1. Hardware Interface Engine (C++17)

自研多协议轮询引擎，采用 `epoll` + `io_uring` 事件驱动模型，单线程管理 256+ 设备连接：

cpp
// pulse_bridge_poller.cpp — simplified core loop
include <liburing.h>

include <sys/epoll.h>

include <vector>

struct DeviceConn {
    int fd;
    uint16_t slave_id;
    std::vector<uint16_t> holding_regs;
};

class PulseBridgePoller {
    int epfd;
    io_uring ring;
    std::vector<DeviceConn> devices;

public:
    void poll_loop() {
        while (true) {
            epoll_event events[256];
            int n = epoll_wait(epfd, events, 256, 10); // 10ms tick
            for (int i = 0; i < n; ++i) {
                auto& dev = devices[events[i].data.fd];
                submit_read_request(ring, dev); // io_uring async read
            }
            io_uring_submit_and_wait(&ring, 1);
            complete_requests(ring);
        }
    }
};


### 2. C++ ↔ Python Zero-Copy Bridge (pybind11)

通过 `pybind11` + NumPy buffer protocol 暴露 C++ 采集缓冲区，避免序列化开销：

cpp
// bridge.cpp
include <pybind11/pybind11.h>

include <pybind11/numpy.h>

namespace py = pybind11;

PYBIND11_MODULE(pulse_bridge, m) {
    py::class_<PulseBridgePoller>(m, "PulseBridgePoller")
        .def(py::init<>())
        .def("poll_once", PulseBridgePoller& self {
            auto& regs = self.latest_regs(); // std::vector<uint16_t>&
            return py::array_t<uint16_t>(
                regs.size(),
                regs.data(),    // zero-copy: no memcpy
                py::capsule(&regs, void p { / no-op, owned by C++ */ })
            );
        });
}


### 3. Backend Microservice (Python / FastAPI)

异步 WebSocket 服务，承载 5,000+ 长连接实时推送设备数据：

python
app.py

import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pulse_bridge import PulseBridgePoller  # pybind11 module

app = FastAPI(title="PulseBridge Gateway")
poller = PulseBridgePoller()
subscribers: set[WebSocket] = set()

@app.websocket("/ws/device/{device_id}")
async def device_stream(ws: WebSocket, device_id: str):
    await ws.accept()
    subscribers.add(ws)
    try:
        while True:
            data = poller.poll_once()  # zero-copy numpy array
            payload = {
                "device_id": device_id,
                "regs": data.tolist(),
                "ts": asyncio.get_event_loop().time()
            }
            await ws.send_json(payload)
            await asyncio.sleep(0.05)  # 20Hz streaming
    except WebSocketDisconnect:
        subscribers.discard(ws)


### 4. AI Agent Orchestration (LangGraph)

将设备抽象为 Tool，Agent 通过状态机自主决策：

python
agent.py

from langgraph.graph import StateGraph, END
from langchain_core.tools import tool

@tool
def read_vitals(device_id: str, metric: str) -> dict:
    """Read real-time vitals from bedside device via PulseBridge."""
    # calls gRPC to edge gateway → C++ poller
    ...

@tool
def trigger_alert(ward: str, level: str, msg: str) -> str:
    """Escalate abnormal reading to nursing station."""
    ...

graph = StateGraph(AgentState)
graph.add_node("sense", sense_node)       # read_vitals
graph.add_node("decide", decide_node)     # LLM reasoning
graph.add_node("act", act_node)           # trigger_alert
graph.add_edge("sense", "decide")
graph.add_conditional_edges("decide", route_decision, {"act": "act", END: END})
graph.add_edge("act", "sense")


---

## 📊 Performance Metrics

| Metric | Before | After (PulseBridge) | Improvement |
|--------|--------|---------------------|-------------|
| Single register read latency | ~15ms (serial) | **2.3ms** (epoll + io_uring) | **6.5×** |
| Backend throughput | 3,800 req/s (sync) | **12,500 req/s** (asyncio) | **3.3×** |
| gRPC p99 latency | 48ms (REST) | **22ms** (gRPC) | **54%↓** |
| Agent tool-call error rate | 18.3% (naive) | **2.1%** (RAG + schema) | **88%↓** |
| WebSocket concurrent conns | 800 | **5,000+** | **6.3×** |
| End-to-end alert latency | 4.2s | **0.8s** | **5.3×** |

---

## 🔍 FAQ (Interview Prep)

**Q: Why C++ for the hardware layer instead of pure Python?**  
A: Python 的 GIL 和 GC 停顿无法满足 256 设备 × 20Hz 轮询的确定性延迟要求。C++ 层保证 2.3ms 单寄存器读取，pybind11 桥接后 Python 侧只做编排——这正是 PulseBridge 的设计哲学：Bridge the gap between real-time hardware and flexible AI orchestration.

**Q: How do you prevent Agent hallucination on tool calls?**  
A: 三层防御：① Tool schema 严格 Pydantic 校验；② RAG 注入设备手册约束参数范围；③ LangGraph 状态机设 fallback 节点，异常自动降级为规则引擎。

**Q: How is data privacy handled?**  
A: 设备数据在 Edge Gateway 完成脱敏（去除患者 PII），仅上传生理参数序列；Agent 推理层部署于院内私有化 vLLM 实例，不触达公网。

---

> ⚠️ **Disclaimer:** This repository is a **portfolio reconstruction** of the PulseBridge project architecture. All patient data, hospital identifiers, and proprietary code have been removed. Performance numbers are from internal benchmarks under controlled lab conditions.

