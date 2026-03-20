import { NextRequest, NextResponse } from "next/server";

/**
 * API route for polishing custom clauses with AI.
 * Uses OPENAI_API_KEY from env. Set in Vercel: Settings → Environment Variables.
 * Called by frontend LOEGenerator. Set OPENAI_API_KEY in env.
 */
export async function POST(req: NextRequest) {
  try {
    const { text } = await req.json();
    if (!text || typeof text !== "string") {
      return NextResponse.json({ error: "Missing or invalid text" }, { status: 400 });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      console.error("OPENAI_API_KEY not set");
      return NextResponse.json(
        { error: "AI polish is not configured. Add OPENAI_API_KEY to Vercel environment variables." },
        { status: 503 }
      );
    }

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        max_tokens: 1000,
        messages: [
          {
            role: "user",
            content: `You are a legal contract drafting assistant for Nonprofit Security Advisors. Rewrite the following rough clause into polished, professional legal contract language. Return ONLY the polished clause text.

Rough clause: ${text}`,
          },
        ],
      }),
    });

    if (!response.ok) {
      const err = await response.text();
      console.error("OpenAI API error:", response.status, err);
      return NextResponse.json(
        { error: "AI service error. Please try again." },
        { status: 502 }
      );
    }

    const data = await response.json();
    const polished =
      data.choices?.[0]?.message?.content?.trim() || "Error generating clause.";

    return NextResponse.json({ polished });
  } catch (err) {
    console.error("Polish clause error:", err);
    return NextResponse.json(
      { error: "Error generating clause." },
      { status: 500 }
    );
  }
}
