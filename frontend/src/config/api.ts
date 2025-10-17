export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://31.97.75.157:4080";

export const SUPPORTED_AGENTS = [
	5, 3, 12, 42, 4, 13, 71, 111, 63, 103, 116, 118, 134, 135, 107, 70, 208, 207,
];

export const AGENT_NAMES: Record<number, string> = {
	5: "Patrick Gardner",
	3: "Agent 3",
	4: "Agent 4",
	12: "Agent 12",
	13: "Bairstow Eves",
	42: "Agent 42",
	63: "BHHS London Properties",
	70: "Fine & Country",
	71: "Hawes & Co",
	103: "Alan de Maid",
	107: "Agent 107",
	111: "The Agency UK",
	116: "Agent 116",
	118: "Agent 118",
	127: "BridgFords",
	134: "Agent 134",
	135: "Agent 135",
	208: "Agent 208",
	207: "Agent 207",
};
