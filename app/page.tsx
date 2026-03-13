export default function Page() {
  return (
    <div style={{ fontFamily: "system-ui", padding: 40, maxWidth: 480 }}>
      <h1 style={{ fontSize: 18, fontWeight: 600, marginBottom: 8 }}>
        LOE Generator API
      </h1>
      <p style={{ fontSize: 14, color: "#666" }}>
        Backend service for engagement letter generation. Use POST /api/polish-clause for AI clause polishing.
      </p>
    </div>
  );
}
