


exports.getBlockId = function(chunkNumber, totalChunks){
    var len = totalChunks.toString().length;
    var n_str = chunkNumber.toString();

    if (n_str.length >= len) {
        return (n_str);
    }

    return (new Array(len + 1).join('0') + chunkNumber).slice(-len);

};