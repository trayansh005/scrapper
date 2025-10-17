"use client";

import { useState } from "react";
import { Play, AlertCircle } from "lucide-react";
import { useScraper } from "../hooks/useScraper";
import { SUPPORTED_AGENTS, AGENT_NAMES } from "../../apiConfig";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

export default function ScraperDashboard() {
	const { runScraper, loading, error } = useScraper();
	const [runningAgents, setRunningAgents] = useState<Set<number>>(new Set());

	const handleRunScraper = async (agentId: number) => {
		setRunningAgents((prev) => new Set([...prev, agentId]));

		const result = await runScraper(agentId);

		if (result) {
			toast.success(`✅ Agent ${agentId} (${AGENT_NAMES[agentId]}) started scraping!`, {
				position: "top-right",
				autoClose: 4000,
			});
		} else if (error) {
			toast.error(`❌ Error: ${error.error}`, {
				position: "top-right",
				autoClose: 4000,
			});
		}

		setRunningAgents((prev) => {
			const updated = new Set(prev);
			updated.delete(agentId);
			return updated;
		});
	};

	const handleRunAll = async () => {
		toast.info("🚀 Starting scraper for all agents...", {
			position: "top-right",
			autoClose: 2000,
		});

		for (const agentId of SUPPORTED_AGENTS) {
			setRunningAgents((prev) => new Set([...prev, agentId]));
			await runScraper(agentId);
			await new Promise((resolve) => setTimeout(resolve, 1000)); // 1 second delay between agents
			setRunningAgents((prev) => {
				const updated = new Set(prev);
				updated.delete(agentId);
				return updated;
			});
		}

		toast.success("✅ All agents scraping jobs queued!", {
			position: "top-right",
			autoClose: 3000,
		});
	};

	return (
		<div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
			<ToastContainer />

			{/* Header */}
			<div className="bg-gradient-to-r from-blue-600 to-purple-600 shadow-lg">
				<div className="max-w-7xl mx-auto px-6 py-12">
					<h1 className="text-4xl font-bold text-white mb-2">🏠 Property Scraper Dashboard</h1>
					<p className="text-blue-100">
						Manage and run property scraping jobs for different agents
					</p>
				</div>
			</div>

			{/* Main Content */}
			<div className="max-w-7xl mx-auto px-6 py-12">
				{/* Stats Cards */}
				<div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
					<div className="bg-slate-700 rounded-lg shadow-lg p-6 border border-slate-600">
						<div className="text-blue-400 text-3xl font-bold mb-2">{SUPPORTED_AGENTS.length}</div>
						<div className="text-slate-300">Total Agents</div>
					</div>
					<div className="bg-slate-700 rounded-lg shadow-lg p-6 border border-slate-600">
						<div className="text-green-400 text-3xl font-bold mb-2">{runningAgents.size}</div>
						<div className="text-slate-300">Running Now</div>
					</div>
					<div className="bg-slate-700 rounded-lg shadow-lg p-6 border border-slate-600">
						<div className="text-purple-400 text-3xl font-bold mb-2">
							{process.env.NEXT_PUBLIC_API_URL || "31.97.75.157:4080"}
						</div>
						<div className="text-slate-300">Backend Server</div>
					</div>
				</div>

				{/* Control Buttons */}
				<div className="bg-slate-700 rounded-lg shadow-lg p-6 mb-8 border border-slate-600">
					<h2 className="text-xl font-bold text-white mb-4">🎮 Controls</h2>
					<button
						onClick={handleRunAll}
						disabled={loading || runningAgents.size > 0}
						className="w-full bg-gradient-to-r from-green-500 to-green-600 hover:from-green-600 hover:to-green-700 disabled:from-slate-500 disabled:to-slate-600 text-white font-bold py-3 px-6 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
					>
						<Play size={20} />
						{loading || runningAgents.size > 0
							? `Running (${runningAgents.size} agents)...`
							: "Run All Agents"}
					</button>
				</div>

				{/* Agents Grid */}
				<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
					{SUPPORTED_AGENTS.map((agentId) => (
						<div
							key={agentId}
							className={`rounded-lg shadow-lg p-6 border transition-all duration-200 ${
								runningAgents.has(agentId)
									? "bg-blue-700 border-blue-500 ring-2 ring-blue-400"
									: "bg-slate-700 border-slate-600 hover:border-slate-500"
							}`}
						>
							<div className="flex items-start justify-between mb-4">
								<div>
									<h3 className="text-lg font-bold text-white">{AGENT_NAMES[agentId]}</h3>
									<p className="text-slate-400 text-sm">Agent ID: {agentId}</p>
								</div>
								{runningAgents.has(agentId) && (
									<div className="animate-spin">
										<Play size={20} className="text-blue-300" />
									</div>
								)}
							</div>

							<button
								onClick={() => handleRunScraper(agentId)}
								disabled={loading || runningAgents.size > 0}
								className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-slate-600 text-white font-semibold py-2 px-4 rounded-lg transition-colors duration-200 flex items-center justify-center gap-2"
							>
								<Play size={16} />
								{runningAgents.has(agentId) ? "Running..." : "Start Scraper"}
							</button>
						</div>
					))}
				</div>

				{/* Error Display */}
				{error && (
					<div className="mt-8 bg-red-900 border border-red-700 rounded-lg p-4 flex gap-3">
						<AlertCircle className="text-red-400 flex-shrink-0" size={24} />
						<div>
							<h3 className="text-red-200 font-bold mb-1">Error</h3>
							<p className="text-red-100">{error.error}</p>
							{error.supportedAgents && (
								<p className="text-red-100 text-sm mt-2">
									Supported agents: {error.supportedAgents.join(", ")}
								</p>
							)}
						</div>
					</div>
				)}

				{/* Info Section */}
				<div className="mt-8 bg-slate-700 rounded-lg shadow-lg p-6 border border-slate-600">
					<h2 className="text-xl font-bold text-white mb-4">ℹ️ Information</h2>
					<div className="space-y-2 text-slate-300 text-sm">
						<p>
							<strong>Backend URL:</strong>{" "}
							{process.env.NEXT_PUBLIC_API_URL || "http://31.97.75.157:4080"}
						</p>
						<p>
							<strong>Total Supported Agents:</strong> {SUPPORTED_AGENTS.length}
						</p>
						<p>
							<strong>Agents:</strong> {SUPPORTED_AGENTS.join(", ")}
						</p>
						<p className="text-yellow-300 mt-4">
							⚠️ Each agent scraping job may take several minutes. Monitor your backend server logs
							for progress.
						</p>
					</div>
				</div>
			</div>
		</div>
	);
}
