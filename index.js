const fs = require("fs");

const async = require("async");
const AWS = require("aws-sdk");

const s3 = new AWS.S3();

module.exports = function (batch, cb) {
    fs.readFile(batch, function (err, data) {
	if (err) return cb(err);

	const prefix = batch.split(".")[0];
	const active = new Map();
	
	async.eachLimit(JSON.parse(data), 6, function (item, cb) {
	    const path = item["S3"].split("/");
	    const pathSplit = [path[0], path.slice(1).join("/")];
	    console.log(pathSplit);
	    const s = fs.createWriteStream(path[path.length - 1]);
	    const os = s3.getObject({
		Bucket: pathSplit[0],
		Key: pathSplit[1]
	    }).createReadStream().pipe(s);
	    os.on("error", function (err) { cb(err); });
	    os.on("finish", function () { cb(); });
	}, function (err) {
	    if (err) {
		async.each(active, function (item, cb) {
		    fs.unlink(item[1], function (err) {
			if (err) console.error(err);
			cb();
		    });
		}, function (unlinkerr) {
		    cb(err);
		});
	    }
	    else {
		cb();
	    }
	});
    });
};
