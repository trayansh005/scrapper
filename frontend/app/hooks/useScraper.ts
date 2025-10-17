"use client";

import { useState, useCallback } from "react";
import axios from "axios";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://31.97.75.157:4080";

export interface ScraperError {
	error: string;
	supportedAgents?: number[];
}

export function useScraper() {
	const [loading, setLoading] = useState(false);
	const [error, setError] = useState<ScraperError | null>(null);

	const runScraper = useCallback(async (agentId: number) => {
		setLoading(true);
		setError(null);

		try {
			await axios.put(
				`${API_BASE_URL}/get-property-url-by-listing-page-and-update-price/${agentId}`,
				{}
			);

			return {
				success: true,
				message: `Agent ${agentId} scraping started`,
				agentId,
			};
		} catch (err) {
			const errorData = axios.isAxiosError(err)
				? err.response?.data
				: { error: "Unknown error occurred" };

			setError(errorData);
			return null;
		} finally {
			setLoading(false);
		}
	}, []);

	return { runScraper, loading, error };
}
