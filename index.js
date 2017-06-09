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
	    const id = item["ID"];
	    const path = item["S3"].split("/");
	    const pathSplit = [path[0], path.slice(1).join("/")];
	    const localPath = path[path.length - 1];

	    fs.access(localPath, fs.constants.F_OK, function (err) {
		if (err) {
		    console.log(pathSplit);
		    active.set(id, localPath);
		    const s = fs.createWriteStream(localPath);
		    const os = s3.getObject({
			Bucket: pathSplit[0],
			Key: pathSplit[1]
		    }, function (err, data) {
			if (err) {
			    cb(err);
			}
			else {
			    active.delete(id);
			    cb();
			}
		    }).createReadStream().pipe(s);
		}
		else {
		    cb();
		}
	    });
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
