import asyncio
import json
from datetime import datetime

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.agents import AgentFinish

from langchain_agent import get_agent, _to_lc_messages

router = APIRouter()

TOOL_CHIPS = {
    "query_analytics_db":          "🔍 Querying database...",
    "get_pipeline_status":         "🔄 Checking pipeline...",
    "trigger_analytics_pipeline":  "▶ Triggering pipeline...",
    "check_infrastructure_health": "🖥️ Checking infrastructure...",
    "get_kafka_topic_info":        "📨 Checking Kafka...",
    "get_flink_job_status":        "⚡ Checking Flink...",
    "get_mysql_row_counts":        "🗄️ Counting rows...",
}


# ── Streaming callback ────────────────────────────────────────────────────────

class SSECallbackHandler(AsyncCallbackHandler):
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue

    async def on_tool_start(self, serialized: dict, input_str: str, **kwargs):
        tool_name = serialized.get("name", "")
        chip = TOOL_CHIPS.get(tool_name, f"🔧 Running {tool_name}...")
        await self.queue.put(
            f"data: {json.dumps({'type': 'tool_start', 'tool': tool_name, 'chip': chip})}\n\n"
        )

    async def on_tool_end(self, output: str, **kwargs):
        await self.queue.put(f"data: {json.dumps({'type': 'tool_end'})}\n\n")

    async def on_agent_finish(self, finish: AgentFinish, **kwargs):
        output = finish.return_values.get("output", "")
        if output:
            await self.queue.put(f"data: {json.dumps({'type': 'token', 'text': output})}\n\n")
        await self.queue.put(f"data: {json.dumps({'type': 'done'})}\n\n")
        await self.queue.put(None)  # sentinel


# ── Request / response models ─────────────────────────────────────────────────

class ChatMessage(BaseModel):
    message: str
    session_id: str = "default"
    history: list[dict] = []


class InsightsResponse(BaseModel):
    insights: str
    generated_at: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/message")
async def chat_message(body: ChatMessage):
    queue: asyncio.Queue = asyncio.Queue()
    handler = SSECallbackHandler(queue)
    lc_history = _to_lc_messages(body.history)

    async def run_agent():
        try:
            agent = get_agent()
            await agent.ainvoke(
                {"input": body.message, "chat_history": lc_history},
                config={"callbacks": [handler]},
            )
        except Exception as e:
            await queue.put(
                f"data: {json.dumps({'type': 'error', 'text': str(e)})}\n\n"
            )
            await queue.put(None)

    async def event_stream():
        asyncio.create_task(run_agent())
        while True:
            item = await queue.get()
            if item is None:
                break
            yield item

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/insights", response_model=InsightsResponse)
async def generate_insights():
    """Run the agent with a fixed prompt to generate a markdown insights report
    covering all 15 analytics tables."""
    prompt = (
        "Generate a comprehensive healthcare analytics insights report. "
        "Query the analytics tables to cover: "
        "1) Financial summary (total revenue, outstanding, top earning doctor, top specialization). "
        "2) Operational summary (total appointments, completion rate, busiest hour, top doctor by score). "
        "3) Patient summary (total patients, retention rate, largest age group, top insurance provider). "
        "Format as markdown with three sections. Be concise — one paragraph per section."
    )
    try:
        agent = get_agent()
        result = await agent.ainvoke({"input": prompt, "chat_history": []})
        return InsightsResponse(
            insights=result.get("output", ""),
            generated_at=datetime.utcnow().isoformat(),
        )
    except Exception as e:
        return InsightsResponse(
            insights=f"Error generating insights: {e}",
            generated_at=datetime.utcnow().isoformat(),
        )
