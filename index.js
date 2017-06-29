const fs = require("fs");
const zlib = require("zlib");

const async = require("async");
const AWS = require("aws-sdk");

const s3 = new AWS.S3();

module.exports = function (batch, cb) {
    fs.readFile(batch, function (err, data) {
	if (err) return cb(err);

	const prefix = batch.split(".")[0];
	
	return async.eachLimit(JSON.parse(data), 6, function (item, cb) {
	    const id = item["ID"];
	    const path = item["S3"].split("/");
	    const pathSplit = [path[0], path.slice(1).join("/")];
	    console.log(pathSplit);

	    async.waterfall([function (cb) {
		s3.headObject({
		    Bucket: pathSplit[0],
		    Key: pathSplit[1]
		}, cb);
	    }, function (objectMetadata, cb) {
		var s = s3.getObject({
		    Bucket: pathSplit[0],
		    Key: pathSplit[1]
		}).createReadStream();

		if (objectMetadata.ContentEncoding) {
		    let cs;
		    if (objectMetadata.ContentEncoding === "gzip") {
			cs = zlib.createGunzip();
		    }
		    else {
			return cb(new Error("Unrecognized Content-Encoding"));
		    }
		    s = s.pipe(cs);
		}

		s = s.pipe(fs.createWriteStream(id + "-" + path[path.length - 1]));
		s.on("error", function (err) { cb(err); });
		s.on("finish", function () { cb(); });
		return null;
	    }], function (err) {
		cb(err);
	    });
	}, function (err) {
	    return cb(err);
	});
    });
};
