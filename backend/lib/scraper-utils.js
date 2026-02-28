/**
 * Memory monitoring utility
 * @param {string} label - Label for the memory log
 */
function logMemoryUsage(label) {
	const used = process.memoryUsage();
	console.log(
		`[${label}] Memory: ${Math.round(used.heapUsed / 1024 / 1024)}MB / ${Math.round(
			used.heapTotal / 1024 / 1024,
		)}MB`,
	);
}

/**
 * Block non-essential resources (images, fonts, stylesheets, media) on a page
 * @param {import('playwright').Page} page
 */
function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
}

module.exports = {
	logMemoryUsage,
	blockNonEssentialResources,
};
