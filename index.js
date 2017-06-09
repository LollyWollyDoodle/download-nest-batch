const fs = require("fs");

const async = require("async");
const AWS = require("aws-sdk");

const s3 = new AWS.S3();

module.exports = function (batch, cb) {
    fs.readFile(batch, function (err, data) {
	if (err) return cb(err);

	const prefix = batch.split(".")[0];
	
	async.each(JSON.parse(data), function (item, cb) {
	    const path = item["S3"].split("/");
	    const pathSplit = [path[0], path.slice(1).join("/")];
	    console.log(pathSplit);
	    const s = fs.createWriteStream(path[path.length - 1]);
	    const os = s3.getObject({
		Bucket: pathSplit[0],
		Key: pathSplit[1]
	    }, function (err, data) {
		if (err) cb(err);
	    }).createReadStream().pipe(s);
	    os.on("error", function (err) { cb(err); });
	    os.on("finish", function () { cb(); });
	}, function (err) {
	    if (err) cb(err);
	});
	
	return cb();
    });
};
