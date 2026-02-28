async function test() {
	const url =
		"https://www.bairstoweves.co.uk/properties/sales/status-available/most-recent-first.ljson?page=1";

	console.log("Testing with curl-like fetch...");
	try {
		const res = await fetch(url, {
			headers: {
				accept: "application/json, text/javascript, */*; q=0.01",
				"x-requested-with": "XMLHttpRequest",
				"User-Agent":
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
			},
		});

		console.log("Status:", res.status);
		console.log("Content-Type:", res.headers.get("content-type"));
		const text = await res.text();
		console.log("Body preview:", text.substring(0, 100));
	} catch (e) {
		console.error("Error:", e);
	}
}
test();
