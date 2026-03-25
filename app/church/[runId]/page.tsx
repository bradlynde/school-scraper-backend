"use client";

import { useParams } from "next/navigation";
import RunDetailPage from "../../../components/RunDetailPage";

export default function ChurchRunDetailPage() {
  const params = useParams();
  const runId = params.runId as string;
  return <RunDetailPage runId={runId} scraperType="church" />;
}
