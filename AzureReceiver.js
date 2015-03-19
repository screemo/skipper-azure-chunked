

var WritableStream = require('stream').Writable;
var azure = require('azure-storage');
var mime = require('mime');
var async = require('async');
var utils = require('./utils');
var domain = require('domain');




var isUploadComplete = function(uncommittedBlocks, totalChunks) {

    var uploadedBlocks = {}, complete = true;

    if (!uncommittedBlocks){
        return complete;
    }

    uncommittedBlocks.forEach(function(block) {
        var b = new Buffer(block.Name, 'base64');
        uploadedBlocks[parseInt(b.toString())] = true;
    });

    for (i = 1; i <= totalChunks; i++){
        if (!uploadedBlocks[i]){
            complete = false;
            break;
        }
    }
    return complete;
};


module.exports = function AzureReceiver(options, adapter) {

    options = options || {};

    options.container = 'container';

    var log = options.log || function noOpLog(){};

    var d = domain.create();

    var blobService = azure.createBlobService( options.storageAccountKey, options.storageAccountSecret );

    options = options || {};
    //options = _.defaults(options, globalOpts);

    var receiver__ = WritableStream({
        objectMode: true
    });

    receiver__.on('error', function (err) {
        console.log('ERROR ON RECEIVER__ ::',err);
    });

    // This `_write` method is invoked each time a new file is received
    // from the Readable stream (Upstream) which is pumping filestreams
    // into this receiver.  (filename === `__newFile.filename`).
    receiver__._write = function onFile(__newFile, encoding, done) {
        var startedAt = new Date();

        __newFile.on('error', function (err) {
            console.log('ERROR ON file read stream in receiver (%s) ::', __newFile.filename, err);
            // TODO: the upload has been cancelled, so we need to stop writing
            // all buffered bytes, then call gc() to remove the parts of the file that WERE written.
            // (caveat: may not need to actually call gc()-- need to see how this is implemented
            // in the underlying knox-mpu module)
            //
            // Skipper core should gc() for us.
        });

        var headers = options.headers || {};

        // Lookup content type with mime if not set
        if ('undefined' === typeof headers['content-type']) {
            headers['content-type'] = mime.lookup(__newFile.fd);
        }

        var container = options.container;
        var blob = __newFile.fd;
        var blockId = utils.getBlockId(options.chunkNumber, options.totalChunks);
        var blockSize = options.chunkSize;
        var totalChunks = options.totalChunks;

        async.auto({

            createContainer: function (cb) {
                blobService.createContainerIfNotExists(container, function (err, result, response) {
                    if (err) {
                        console.log(('Receiver: Error creating container ' + container + ' :: Cancelling upload and cleaning up already-written bytes ... ' ).red);
                        cb(err);
                        return;
                    }

                    return cb(null);
                });
            },
            uploadBlock: ['createContainer', function (cb, results) {
                console.log(blob + ' ' + blockId);
                d.run(function () {
                    blobService.createBlockFromStream(blockId, container, blob, __newFile, blockSize,
                        function (error, response) {
                            if (error) {
                                console.log(('Receiver: Error during blob upload  ' + blob + ' :: Cancelling upload and cleaning up already-written bytes ... ' ).red);
                                cb(error);
                                return;
                            }

                            __newFile.extra = response;
                            __newFile.size = new Number(__newFile.size);

                            var endedAt = new Date();
                            var duration = ((endedAt - startedAt) / 1000);
                            console.log('**** Azure upload took ' + duration + ' seconds...' + blockId);
                            return cb(null);

                        })
                });
            }],
            uncommittedBlocks: ['uploadBlock', function (cb, results) {
                blobService.listBlocks(container, blob, 'uncommitted', function (error, list, response) {
                    if (error) {
                        console.log('Error listing blocks ' + blob + ' :: ' + error);
                        return cb(error);

                    }

                    if (!list) {
                        return cb(null);
                    }

                    return cb(null, list.UncommittedBlocks);
                });
            }],
            commitBlob: ['uncommittedBlocks', function (cb, results) {

                if (!isUploadComplete(results.uncommittedBlocks, totalChunks)) {
                    return cb(null);
                }

                console.log('Upload complete. About to commit blob!');


                var blockList = [];

                for (var i = 1; i <= totalChunks; i++) {
                    blockList.push(utils.getBlockId(i, totalChunks));
                }
                console.log(blockList);
                blobService.commitBlocks(container, blob, {UncommittedBlocks: blockList}, function (err, list, response) {
                    console.log('Upload complete');
                    return cb(err);
                });
            }]
        }, function (err, results) {
            if (err) {
                return receiver__.emit('error', err);
            }

            return done();
        });
    };

    return receiver__;
};