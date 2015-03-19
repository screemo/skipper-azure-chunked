/**
 * Module dependencies
 */

var path = require('path');
var AzureReceiver = require('./AzureReceiver');
var azure = require('azure-storage');
var utils = require('./utils');


/**
 * skipper-azure
 *
 * @type {Function}
 * @param  {Object} options
 * @return {Object}
 */

module.exports = function AzureStore(options) {
    options = options || {};

    var log = options.log || function _noOpLog() {};



    options.container = options.container || 'container';

    var adapter = {};

    adapter.rm = function(fd, cb) {

        var blobService = azure.createBlobService(options.storageAccountKey, options.storageAccountSecret);

        blobService.deleteBlobIfExists( options.container, fd, function( err, result, response ){
            if( err ) {
                return cb( err );
            }

            // construct response
            cb( null, {
                filename: fd,
                success: true,
                extra: response
            });
        });

    };

    adapter.ls = function(dirname, cb) {

        if ( !dirname ) {
            dirname = '/';
        }

        var prefix = dirname;

        var blobService = azure.createBlobService(options.storageAccountKey, options.storageAccountSecret);

        blobService.listBlobsSegmentedWithPrefix( options.container, prefix,
            null, function( err, result, response ) {
                if( err ) {
                    return cb( err );
                }

                var data = _.pluck( result.entries, 'name');
                data = _.map(data, function snipPathPrefixes (thisPath) {
                    thisPath = thisPath.replace(/^.*[\/]([^\/]*)$/, '$1');

                    // Join the dirname with the filename
                    thisPath = path.join(dirname, path.basename(thisPath));

                    return thisPath;
                });
                cb( null, data );
            })

    };

    adapter.read = function(fd, cb) {

        var prefix = fd;

        // Build a noop transform stream that will pump the Azure output through
        var __transform__ = new Transform();
        __transform__._transform = function (chunk, encoding, callback) {
            return cb(null, chunk);
        };

        var blobService = azure.createBlobService(options.storageAccountKey, options.storageAccountSecret);

        blobService.getBlobToStream( options.container, prefix, __transform__, function( err, result, response ) {
            if ( err ) {
                cb( err );
            }

            __transform__.emit( 'finish', err, result.blob)
        });

        return __transform__
    };

    adapter.chunkExists = function(callback) {
        var blobService = azure.createBlobService(options.storageAccountKey, options.storageAccountSecret);


        blobService.listBlocks(options.container, options.saveAs, 'uncommitted', function(error, list, response){
            if (error) {
                console.log('Error listing blocks ' + blob + ' :: ' + error);
                return callback(error);

            }

            if (!list || !list.UncommittedBlocks) {
                return callback(false);
            }

            var blockId = utils.getBlockId(options.chunkNumber, options.totalChunks);

            async.detect(list.UncommittedBlocks, function(item, next) {
                var b = new Buffer(item.Name, 'base64');
                return next(b.toString() === blockId);
            }, callback);

        });
    };

    adapter.receive = AzureReceiver;

    return adapter;
};




