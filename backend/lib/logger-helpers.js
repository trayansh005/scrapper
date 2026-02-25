function createAgentLogger(agentId) {
	function prefix(pageNum = null, label = null) {
		const agentPart = `[Agent ${agentId}]`;
		const pagePart = pageNum !== null && pageNum !== undefined ? `[Page ${pageNum}]` : "";
		const labelPart = label ? `[${label}]` : "";
		return `${agentPart}${pagePart}${labelPart}`;
	}

	function step(message) {
		console.log(`ℹ️  ${prefix()} ${message}`);
	}

	function page(pageNum, label, message) {
		console.log(`📄 ${prefix(pageNum, label)} ${message}`);
	}

	function property(pageNum, label, title, price, url, isRental = false) {
		const type = isRental ? "LETTINGS" : "SALES";
		console.log(`🏠 ${prefix(pageNum, label)} [${type}] ${title} - ${price} - ${url}`);
	}

	function error(message, err = null, pageNum = null, label = null) {
		const suffix = err?.message ? ` | ${err.message}` : "";
		console.error(`❌ ${prefix(pageNum, label)} ${message}${suffix}`);
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
