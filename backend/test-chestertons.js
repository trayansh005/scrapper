async function test() {
	const params = {
		search: { channel: "sales", status: "available" },
		page: 1,
		pageSize: 12,
	};
	const url = `https://www.chestertons.co.uk/api/properties?params=${encodeURIComponent(JSON.stringify(params))}`;
	console.log("Fetching URL:", url);

	try {
		const res = await fetch(url, {
			headers: {
				accept: "application/json, text/plain, */*",
			},
		});

		console.log("Status:", res.status);
		console.log("Content-Type:", res.headers.get("content-type"));

		if (res.ok) {
			const data = await res.json();
			console.log("Success! Properties found:", data.results ? data.results.length : 0);
			if (data.results && data.results.length > 0) {
				console.log("First property:", data.results[0].displayAddress, data.results[0].priceValue);
			}
		} else {
			console.log("Failed. Body preview:", (await res.text()).substring(0, 200));
		}
	} catch (err) {
		console.error("Error:", err);
	}
}
test();
