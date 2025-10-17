import { useState, useCallback } from 'react';
import axios from 'axios';
import { API_BASE_URL } from '../../apiConfig';

export interface ScraperResponse {
	success: boolean;
	message: string;
	agentId: number;
}

export interface ScraperError {
	error: string;
	supportedAgents?: number[];
}

export function useScraper() {
	const [loading, setLoading] = useState(false);
	const [error, setError] = useState<ScraperError | null>(null);
	const [success, setSuccess] = useState(false);

	const runScraper = useCallback(
		async (agentId: number): Promise<ScraperResponse | null> => {
			setLoading(true);
			setError(null);
			setSuccess(false);

			try {
				await axios.put(
					`${API_BASE_URL}/get-property-url-by-listing-page-and-update-price/${agentId}`,
					{}
				);

				setSuccess(true);
				return {
					success: true,
					message: `Agent ${agentId} scraping started`,
					agentId,
				};
			} catch (err) {
				const errorData = axios.isAxiosError(err)
					? err.response?.data
					: { error: 'Unknown error occurred' };

				setError(errorData);
				return null;
			} finally {
				setLoading(false);
			}
		},
		[]
	);

	return { runScraper, loading, error, success };
}
