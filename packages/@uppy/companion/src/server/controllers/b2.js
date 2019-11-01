const router = require('express').Router
// const ms = require('ms')

const MAX_ENDPOINTS_PER_FILE = 5

module.exports = function b2 (config) {
  if (typeof config.getPath !== 'function') {
    throw new TypeError('b2: the `getPath` option must be a function')
  }

  /*
  function getUploadParameters (req, res, next) {
    const client = req.uppy.b2Client
    const key = config.getKey(req, req.query.filename)
    if (typeof key !== 'string') {
      return res.status(500).json({ error: 's3: filename returned from `getKey` must be a string' })
    }
    const fields = {
      // keyId: keyId,
      key: key,
      success_action_status: '201',
      'content-type': req.query.type
    }
  }
*/

  const getCachedBucketID = (() => {
    const cache = Object.create({})
    return (client, bucketName) => {
      const match = cache[bucketName]
      if (match && match.expiration < Date.now()) {
        return match.result
      } else {
        return (cache[bucketName] = {
          result: client.getBucketId({ bucketName }),
          expiration: Date.now() + (config.cacheBucketIdDurationMS || 3600)
        }).result
      }
    }
  })()

  /**
   * Initiate a B2 large file upload.
   *
   * Expected JSON body:
   *  - filename - The name of the file, given to the `config.getPath`
   *    option to determine the object's final path in the B2 bucket.
   *  - type - The MIME type of the file
   *
   * Response JSON:
   *  - fileId - The unique B2 fileId which will be used again to initiate
   *    the actual file transfer.
   */
  function createMultipartUpload (req, res, next) {
    const client = req.uppy.b2Client
    const fileName = config.getPath(req, req.body.filename)
    const { type } = req.body

    if (typeof fileName !== 'string') {
      return res.status(500).json({ error: 'b2: filename returned from `getPath` must be a string' })
    }
    if (typeof type !== 'string') {
      return res.status(400).json({ error: 'b2: content type must be a string' })
    }

    return getCachedBucketID(client, config.bucket)
      .then(bucketId =>
        client.startLargeFile({
          bucketId,
          fileName,
          contentType: type
        }))
      .then(largeFileResponse => ({
        fileId: largeFileResponse.fileId,
        bucketId: largeFileResponse.bucketId
      }))
      .then((data) => {
        res.json(data)
      })
      .catch(err => next(err))
  }

  /**
   *
   */
  function getEndpoint (req, res, next) {
    const client = req.uppy.b2Client
    const { fileId } = req.body

    if (typeof fileId !== 'string') {
      return res.status(400).json({ error: 'b2: fileId type must be a string' })
    }

    client.getUploadPartURL({ fileId })
      .then(data => res.json(data))
      .catch(err => next(err))
  }

  return router()
    // .get('/large/:bucketName/:uploadId', start_large)
    // .get('/params', getUploadParameters)
    .post('/multipart', createMultipartUpload)
    .post('/multipart/endpoint', getEndpoint)
    // .get('/multipart/:uploadId', getUploadedParts)
    // .get('/multipart/:uploadId/:partNumber', signPartUpload)
    // .post('/multipart/:uploadId/complete', completeMultipartUpload)
    // .delete('/multipart/:uploadId', abortMultipartUpload)
}
