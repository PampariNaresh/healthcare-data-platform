import os
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from tools import ALL_TOOLS

SYSTEM_PROMPT = """You are a healthcare data platform assistant for a hospital network \
running on AWS EC2.

You have access to tools that connect to live systems:
- MySQL database (30 tables: 10 operational + 20 analytics_* tables pre-aggregated by Spark)
- Apache Airflow (orchestrates daily batch analytics pipeline)
- Apache Flink (2 real-time streaming jobs processing 10 Kafka topics)
- Apache Kafka (10 topics: patients, doctors, appointments, treatments, billing,
    departments, patient_vitals, lab_reports, hospital_events, icu_codes)
- Infrastructure health checks for all services

Database schema overview:
  Operational tables:
    patients, doctors, appointments, treatments, billing,
    departments, patient_vitals, lab_reports, hospital_events, icu_codes

  Analytics tables — Financial (updated daily by Spark):
    analytics_revenue_by_doctor, analytics_revenue_by_specialization,
    analytics_revenue_by_branch, analytics_billing_payment,
    analytics_outstanding_payments, analytics_monthly_revenue, analytics_treatment_cost

  Analytics tables — Operational:
    analytics_appointment_status, analytics_doctor_workload,
    analytics_peak_hours, analytics_top_doctors_scorecard

  Analytics tables — Patient:
    analytics_patient_spending, analytics_patient_age_groups,
    analytics_patient_retention, analytics_new_patient_trend

  Analytics tables — Monitoring:
    analytics_vitals_patient_summary  (anomaly rates per patient)
    analytics_lab_test_summary        (flag distribution per test: normal/low/high/critical)
    analytics_hospital_event_summary  (counts & revenue per event type)
    analytics_department_activity     (events + ICU codes + revenue per department)
    analytics_icu_code_summary        (ICU code activations by type & severity)

Rules:
- Prefer analytics_* tables for KPI and aggregation questions (faster, pre-aggregated).
- For monitoring questions (anomalies, ICU codes, lab flags, department load) use get_monitoring_summary first — it gives a full snapshot in one call.
- Use operational tables only when analytics tables cannot answer the question.
- Format currency as ₹X,XXX or ₹X.XM. Use markdown tables for multi-row results.
- NEVER run INSERT, UPDATE, DELETE, DROP, or any data-modifying SQL.
- Before calling trigger_analytics_pipeline, confirm with the user that they want to run it.
- Always respond in clear, human-readable language. Never show raw SQL, JSON, or Python dicts in your reply — summarise the data as sentences or markdown tables.
- Be concise. Lead with the answer, then show supporting data.
- If a query returns many rows, summarise the top results and note the total count."""

_TOOL_MAP = {t.name: t for t in ALL_TOOLS}


def _to_lc_messages(history: list[dict]):
    msgs = []
    for m in history:
        role = m.get("role", "")
        content = m.get("content", "")
        if role == "user":
            msgs.append(HumanMessage(content=content))
        elif role == "assistant":
            msgs.append(AIMessage(content=content))
    return msgs


def _make_llm() -> ChatGroq:
    return ChatGroq(
        model="meta-llama/llama-4-scout-17b-16e-instruct",
        api_key=os.getenv("GROQ_API_KEY"),
        streaming=False,
        max_tokens=2048,
        temperature=0,
    )


async def run_agent(input_text: str, chat_history: list) -> str:
    """Run a manual tool-calling loop with retry on failed_generation."""
    from groq import APIError as GroqAPIError

    llm = _make_llm()
    llm_with_tools = llm.bind_tools(ALL_TOOLS)

    messages = [SystemMessage(content=SYSTEM_PROMPT)] + chat_history + [
        HumanMessage(content=input_text)
    ]

    for _ in range(8):
        try:
            response = await llm_with_tools.ainvoke(messages)
        except GroqAPIError as e:
            # Model generated a malformed tool call — inject a correction and retry
            if "tool_use_failed" in str(e) or "failed_generation" in str(e):
                messages.append(HumanMessage(
                    content="Your previous tool call had invalid JSON. "
                            "Please call the tool again using strict JSON format "
                            'with quoted keys and string values, e.g. {"sql": "SELECT ..."}.'
                ))
                continue
            raise

        if not response.tool_calls:
            return response.content or ""

        messages.append(response)
        for tc in response.tool_calls:
            tool = _TOOL_MAP.get(tc["name"])
            if tool is None:
                result = f"Unknown tool: {tc['name']}"
            else:
                try:
                    result = tool.invoke(tc["args"] or {})
                except Exception as e:
                    result = f"Tool error: {e}"
            messages.append(ToolMessage(
                content=str(result),
                tool_call_id=tc["id"],
            ))

    return "I reached the maximum number of steps. Please try a more specific question."


# Compatibility shim for the insights endpoint
class _FakeAgentExecutor:
    async def ainvoke(self, inputs: dict) -> dict:
        output = await run_agent(
            inputs["input"],
            inputs.get("chat_history", []),
        )
        return {"output": output}


def get_agent() -> _FakeAgentExecutor:
    """Returns an object with ainvoke() compatible with the chat router."""
    return _FakeAgentExecutor()
