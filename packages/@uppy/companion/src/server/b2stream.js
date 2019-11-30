const fdSlicer = require('fd-slicer')
const crypto = require('crypto')
const Writable = require('stream').Writable
const fs = require('fs')

const MAX_UPLOAD_PARTS = 10000

module.exports = class B2Stream {
  // bucketName
  // path
  // fileName
  // fileSize
  // stream
  // client
  // endpointPool
  constructor (client, options) {
    this.options = options
    this.client = client
    this.onUploadProgress = ({ loaded, total }) => {}
  }

  getOptimalChunkSize (fileSize) {
    return this.client.preauth()
      .then(({ recommendedPartSize }) => {
        return Math.max(recommendedPartSize, Math.ceil(fileSize / MAX_UPLOAD_PARTS))
      })
  }

  getBucketId (bucketName) {
    return this.client.getCachedBucket(bucketName)
      .then(({ bucketId }) => bucketId)
  }

  send (cb) {
    return Promise.all([
      this.getOptimalChunkSize(this.options.fileSize),
      this.getBucketId(this.options.bucketName)
    ]).then(([chunkSize, bucketId]) => {
      const isMultipart = (chunkSize < this.options.fileSize)
      if (isMultipart) {
        return this._sendMultipart({ chunkSize, bucketId })
      } else {
        return this._sendSingle({ chunkSize, bucketId })
      }
    })
      .catch(err => {
        console.log('send error', err)
        cb(err)
      })
      .then(result => cb(null, result))
  }

  _sendMultipart ({ chunkSize, bucketId }) {
    const { fileName } = this.options
    const largeFile = this.client.startLargeFile({ bucketId, fileName })
      .then(({ data }) => data)

    const transmit = largeFile
      .then(data => {
        const { stream, path, endpointPool } = this.options
        const { client } = this

        return new Promise((resolve, reject) => {
          const chunks = []
          const writerOptions = {
            client,
            endpointPool,
            chunkSize,
            path,
            // bucketId: this.options.bucketId,
            handleSent: chunk => {
              chunks[chunk.id] = { hash: chunk.hash }
            }
          }

          stream
            .pipe(B2StreamWriter(writerOptions))
            .on('end', () => {
              // TODO -- verify successful upload!
              resolve(chunks)
            })
        })
      })

    const finish = transmit
      .then(chunks => {
        console.log('finishing', chunks)
        return chunks
      })

    return finish
    // b2_start_large_file (bucketId, fileName, contentType) -> fileId
    // start streaming
    //    b2_get_upload_part_url (fileId) -> authorizationToken, url
    //    b2_upload_part (authorizationToken, url)
    // finish
    // b2_finish_large_file (fileId, partSha1Array)
  }

  _sendSingle ({ chunkSize, bucketId }) {
    // b2_get_upload_url (bucketId) -> authorizationTokem, url
    // b2_upload_file (authorizationToken, url, hash)
    //
    // console.log('SEND SINGLE')
    // return Promise.resolve({ done: true })

    const transmit = new Promise((resolve, reject) => {
      const { stream, path, endpointPool } = this.options
      const { client } = this
      const chunks = []

      const writerOptions = {
        client,
        endpointPool,
        chunkSize,
        path,
        handleSent: chunk => {
          chunks[chunk.id] = { hash: chunk.hash }
        }
      }

      try {
        stream
          .pipe(B2StreamWriter(writerOptions))
          .on('end', () => {
            // TODO -- verify successful upload!
            resolve(chunks)
          })
      } catch (err) {
        reject(err)
      }
    })

    return transmit
  }
}

function B2StreamWriter ({ client, endpointPool, connections = 5, chunkSize, path, handleSent }) {
  let accum = 0 // total bytes received
  let chunkAccum = 0 // total bytes in the current which have been processed
  // let sentAccum = 0 // total bytes transmitted to Backblaze
  let chunkCount = 0 // number of emitted chunks

  // Create a new promise which will resolve to a fd-slicer instance
  const slicer = new Promise((resolve, reject) => {
    fs.open(path, 'r', (err, fd) => {
      if (err) {
        reject(err)
      }
      const slicer = fdSlicer.createFromFd(fd)
      resolve({ slicer, fd })
    })
  })

  const workers = []
  const queue = []

  const pendingWriteDrain = () => {
    // If we have pending onwrite()s queued and we're under the maximum
    // number of transmit workers...
    if (queue.length && workers.length < connections) {
      const cb = queue.shift()
      return cb() // signal that we're ready for more data
    }
  }

  const pendingWriteHandler = (onwrite) => {
    queue.push(onwrite)
    return pendingWriteDrain()
  }

  const emit = (stream) => {
    const start = accum - chunkAccum
    const end = accum
    const id = chunkCount++
    chunkAccum = 0

    const transmit = new Promise((resolve) => {
      console.log('requesting slicer')
      slicer.then(({ slicer }) => {
        // Create a new SHA1 hasher
        const hasher = crypto.createHash('sha1')
        hasher.setEncoding('hex')

        console.log('slicin?', slicer)
        // Create a new read stream for this segment
        // and pipe it to the sha1 hasher.
        slicer.createReadStream({ start, end })
          .on('end', () => {
            hasher.end()
            console.log('finished hashing chunk', id)
            resolve({
              hash: hasher.read(),
              stream: slicer.createReadStream({ start, end })
            })
          })
          .pipe(hasher)
      })
    }).then(({ hash, stream }) => {
      if (handleSent) {
        console.log('transmitted', id, hash)
        handleSent({ id, hash })
      }
    }).then(response => {
      // Transmission complete, remove this promise
      // from the array of worker chunk transmissions
      const index = workers.indexOf(transmit)
      workers.splice(index, 1)

      // Keep the stream moving
      pendingWriteDrain()

      return response
    })

    // Push this transmission promise into the pending worker queue
    workers.push(transmit)
    return transmit
  }

  return new Writable({
    write: function (chunk, enc, cb) {
      let remaining = chunk.length
      while (remaining > 0) {
        const maxRead = Math.min(remaining, chunkSize - chunkAccum)
        remaining -= maxRead
        chunkAccum += maxRead
        accum += maxRead
        if (chunkAccum === chunkSize) {
          emit(this)
        }
      }

      console.log('write', chunk && chunk.length)
      // handle the callback (now or later)
      pendingWriteHandler(cb)
    },
    final: function (cb) {
      console.log('final')
      emit(this)

      console.log('waiting on workers to complete', workers.length)

      Promise.all(workers)
        .then(() => slicer)
        .then(({ fd }) => new Promise((resolve, reject) => {
          fs.close(fd, (err) => {
            if (err) {
              reject(err)
              return
            }
            console.log('closed fd', fd)
            resolve(fd)
          })
        }))
        .then(cb)
    }
  })
}
