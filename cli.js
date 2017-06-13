#!/usr/bin/env node
const f = require("./index");

f(process.argv[2], function (err) {
    if (err) {
	console.error(err);
    }
});
