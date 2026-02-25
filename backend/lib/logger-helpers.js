function createAgentLogger(agentId) {
	function normalizeLabel(label) {
		if (!label) return "";
		return String(label)
			.replace(/_PAGE_\d+$/i, "")
			.replace(/\s+/g, " ")
			.trim();
	}

	function prefix(pageNum = null, label = null, totalPages = null) {
		const agentPart = `[🤖 Agent ${agentId}]`;
		const pagePart =
			pageNum !== null && pageNum !== undefined
				? `[P${pageNum}${totalPages ? `/${totalPages}` : ""}]`
				: "";
		const cleanLabel = normalizeLabel(label);
		const labelPart = cleanLabel ? `[${cleanLabel}]` : "";
		return `${agentPart}${pagePart}${labelPart}`;
	}

	function step(message) {
		console.log(`🔹 ${prefix()} ${message}`);
	}

	function page(pageNum, label, message, totalPages = null) {
		console.log(`📄 ${prefix(pageNum, label, totalPages)} ┃ ${message}`);
	}

	function actionBadge(action = "SEEN") {
		switch (action) {
			case "CREATED":
				return "🆕 CREATED";
			case "UPDATED":
				return "✏️ UPDATED";
			case "ERROR":
				return "⚠️ ERROR";
			case "SEEN":
			default:
				return "👀 SEEN";
		}
	}

	function property(
		pageNum,
		label,
		title,
		price,
		url,
		isRental = false,
		totalPages = null,
		action = "SEEN",
	) {
		const icon = isRental ? "🏢" : "🏡";
		console.log(
			`${icon} ${prefix(pageNum, label, totalPages)} ${actionBadge(action)} ✦ ${title} • ${price}`,
		);
		console.log(`   🔗 ${url}`);
	}

	function error(message, err = null, pageNum = null, label = null) {
		const suffix = err?.message ? ` | ${err.message}` : "";
		console.error(`🚨 ${prefix(pageNum, label)} ${message}${suffix}`);
	}

	return {
		step,
		page,
		property,
		error,
	};
}

module.exports = {
	createAgentLogger,
};
