const sha1 = require('js-sha1')

const MB = 1024 * 1024

const defaultOptions = {
  limit: 1,
  onStart () {},
  onProgress () {},
  onPartComplete () {},
  onSuccess () {},
  onError (err) {
    throw err
  }
}

function remove (arr, el) {
  const i = arr.indexOf(el)
  if (i !== -1) arr.splice(i, 1)
}

class MultipartUploader {
  constructor (file, options) {
    this.options = {
      ...defaultOptions,
      ...options
    }
    this.file = file

    this.key = this.options.key || null
    this.uploadId = this.options.uploadId || null
    this.parts = this.options.parts || []

    // Do `this.createdPromise.then(OP)` to execute an operation `OP` _only_ if the
    // upload was created already. That also ensures that the sequencing is right
    // (so the `OP` definitely happens if the upload is created).
    //
    // This mostly exists to make `_abortUpload` work well: only sending the abort request if
    // the upload was already created, and if the createMultipartUpload request is still in flight,
    // aborting it immediately after it finishes.
    this.createdPromise = Promise.reject() // eslint-disable-line prefer-promise-reject-errors
    this.isPaused = false
    this.chunks = null
    this.chunkState = null
    this.uploading = []

    this._initChunks()

    this.createdPromise.catch(() => {}) // silence uncaught rejection warning
  }

  _initChunks () {
    const chunks = []
    const chunkSize = Math.max(Math.ceil(this.file.size / 10000), 5 * MB)

    for (let i = 0; i < this.file.size; i += chunkSize) {
      const end = Math.min(this.file.size, i + chunkSize)
      chunks.push(this.file.slice(i, end))
    }

    this.chunks = chunks
    this.chunkState = chunks.map(() => ({
      uploaded: 0,
      busy: false,
      done: false
    }))
  }

  _createUpload () {
    this.createdPromise = Promise.resolve().then(() =>
      this.options.createMultipartUpload()
    )
    return this.createdPromise.then((result) => {
      const valid = typeof result === 'object' && result &&
        typeof result.fileId === 'string'
      if (!valid) {
        throw new TypeError('BackblazeB2/Multipart: Got incorrect result from `createMultipartUpload()`, expected an object `{ fileId, uploadUrl, authorizationToken }`.')
      }

      this.fileId = result.fileId
      this.endpoints = []

      this.options.onStart(result)
      this._uploadParts()
    }).catch((err) => {
      this._onError(err)
    })
  }

  // TODO
  _resumeUpload () {
    return Promise.resolve().then(() =>
      this.options.listParts({
        uploadId: this.uploadId,
        key: this.key
      })
    ).then((parts) => {
      parts.forEach((part) => {
        const i = part.PartNumber - 1
        this.chunkState[i] = {
          uploaded: part.Size,
          done: true
        }

        // Only add if we did not yet know about this part.
        if (!this.parts.some((p) => p.PartNumber === part.PartNumber)) {
          this.parts.push({
            PartNumber: part.PartNumber
          })
        }
      })
      this._uploadParts()
    }).catch((err) => {
      this._onError(err)
    })
  }

  _uploadParts () {
    if (this.isPaused) return

    const need = this.options.limit - this.uploading.length
    if (need === 0) return

    console.log('NEED', need)
    // All parts are uploaded.
    if (this.chunkState.every((state) => state.done)) {
      this._completeUpload()
      return
    }

    const candidates = []
    for (let i = 0; i < this.chunkState.length; i++) {
      const state = this.chunkState[i]
      if (state.done || state.busy) continue

      candidates.push(i)
      if (candidates.length >= need) {
        break
      }
    }

    candidates.forEach((index) => {
      this._uploadPart(index)
    })
  }

  _uploadPart (index) {
    this.chunkState[index].busy = true

    // Ensure the sha1 has been calculated for this part
    if (typeof this.chunkState[index].sha1 === 'undefined') {
      this.chunkState[index].sha1 = this._getPartSha1Sum(index)
    }

    return Promise.all([
      this._endpointAcquire(),
      this.chunkState[index].sha1
    ]).then(
      ([endpoint, sha1]) =>
        this._uploadPartBytes(index, endpoint, sha1),
      (err) => this._onError(err)
    )
  }

  _uploadPart2 (index) {
    const body = this.chunks[index]
    this.chunkState[index].busy = true

    return Promise.resolve().then(() =>
      this.options.prepareUploadPart({
        key: this.key,
        uploadId: this.uploadId,
        body,
        number: index + 1
      })
    ).then((result) => {
      const valid = typeof result === 'object' && result &&
        typeof result.url === 'string'
      if (!valid) {
        throw new TypeError('AwsS3/Multipart: Got incorrect result from `prepareUploadPart()`, expected an object `{ url }`.')
      }
      return result
    }).then(({ url }) => {
      this._uploadPartBytes(index, url)
    }, (err) => {
      this._onError(err)
    })
  }

  _onPartProgress (index, sent, total) {
    this.chunkState[index].uploaded = sent

    const totalUploaded = this.chunkState.reduce((n, c) => n + c.uploaded, 0)
    this.options.onProgress(totalUploaded, this.file.size)
  }

  _onPartComplete (index) {
    this.chunkState[index].done = true

    const part = {
      PartNumber: index + 1
    }
    this.parts.push(part)

    this.options.onPartComplete(part)

    this._uploadParts()
  }

  _getPartSha1Sum (index) {
    const body = this.chunks[index]
    return body.arrayBuffer()
      .then(buffer => sha1(buffer))
      .then(sha1sum => (this.chunkState[index].sha1 = sha1sum))
  }

  _uploadPartBytes (index, endpoint, sha1) {
    const body = this.chunks[index]
    const xhr = new XMLHttpRequest()
    xhr.open('POST', endpoint.uploadUrl, true)
    xhr.responseType = 'json'
    xhr.setRequestHeader('Authorization', endpoint.authorizationToken)
    xhr.setRequestHeader('X-Bz-Part-Number', index + 1)
    xhr.setRequestHeader('X-Bz-Content-Sha1', sha1)
    xhr.setRequestHeader('Content-Length', body.size)
    this.uploading.push(xhr)

    console.log('ep', endpoint)
    xhr.upload.addEventListener('progress', (ev) => {
      if (!ev.lengthComputable) return
      this._onPartProgress(index, ev.loaded, ev.total)
    })

    xhr.addEventListener('abort', (ev) => {
      remove(this.uploading, ev.target)
      this.chunkState[index].busy = false
      this._endpointRelease(endpoint)
    })

    xhr.addEventListener('load', (ev) => {
      remove(this.uploading, ev.target)
      this.chunkState[index].busy = false

      if (ev.target.status < 200 || ev.target.status >= 300) {
        this._onError(new Error('Non 2xx'))
        return
      }

      this._onPartProgress(index, body.size, body.size)

      // NOTE This must be allowed by CORS.
      // const etag = ev.target.getResponseHeader('ETag')
      // if (etag === null) {
      //   this._onError(new Error('AwsS3/Multipart: Could not read the ETag header. This likely means CORS is not configured correctly on the S3 Bucket. Seee https://uppy.io/docs/aws-s3-multipart#S3-Bucket-Configuration for instructions.'))
      //   return
      // }

      this._endpointRelease(endpoint)
      this._onPartComplete(index)
    })

    xhr.addEventListener('error', (ev) => {
      remove(this.uploading, ev.target)
      this.chunkState[index].busy = false

      const error = new Error('Unknown error')
      error.source = ev.target
      this._onError(error)
    })

    xhr.send(body)
  }

  _uploadPartBytes2 (index, url) {
    const body = this.chunks[index]
    const xhr = new XMLHttpRequest()
    xhr.open('PUT', url, true)
    xhr.responseType = 'text'

    this.uploading.push(xhr)

    xhr.upload.addEventListener('progress', (ev) => {
      if (!ev.lengthComputable) return

      this._onPartProgress(index, ev.loaded, ev.total)
    })

    xhr.addEventListener('abort', (ev) => {
      remove(this.uploading, ev.target)
      this.chunkState[index].busy = false
    })

    xhr.addEventListener('load', (ev) => {
      remove(this.uploading, ev.target)
      this.chunkState[index].busy = false

      if (ev.target.status < 200 || ev.target.status >= 300) {
        this._onError(new Error('Non 2xx'))
        return
      }

      this._onPartProgress(index, body.size, body.size)

      // NOTE This must be allowed by CORS.
      const etag = ev.target.getResponseHeader('ETag')
      if (etag === null) {
        this._onError(new Error('AwsS3/Multipart: Could not read the ETag header. This likely means CORS is not configured correctly on the S3 Bucket. Seee https://uppy.io/docs/aws-s3-multipart#S3-Bucket-Configuration for instructions.'))
        return
      }

      this._onPartComplete(index, etag)
    })

    xhr.addEventListener('error', (ev) => {
      remove(this.uploading, ev.target)
      this.chunkState[index].busy = false

      const error = new Error('Unknown error')
      error.source = ev.target
      this._onError(error)
    })

    xhr.send(body)
  }

  _completeUpload () {
    // Parts may not have completed uploading in sorted order, if limit > 1.
    this.parts.sort((a, b) => a.PartNumber - b.PartNumber)

    // Build part sha1 checksum array
    const sha1Sums = Promise.all(
      this.chunkState
        .map(chunkState => chunkState.sha1)
    )

    sha1Sums.then((partSha1Array) =>
      this.options.completeMultipartUpload({
        fileId: this.fileId,
        parts: this.parts,
        partSha1Array
      })
    ).then((result) => {
      this.options.onSuccess(result)
    }, (err) => {
      this._onError(err)
    })
  }

  _abortUpload () {
    this.uploading.slice().forEach(xhr => {
      xhr.abort()
    })
    this.createdPromise.then(() => {
      this.options.abortMultipartUpload({
        key: this.key,
        uploadId: this.uploadId
      })
    }, () => {
      // if the creation failed we do not need to abort
    })
    this.uploading = []
  }

  _onError (err) {
    this.options.onError(err)
  }

  _endpointAcquire () {
    return new Promise((resolve, reject) => {
      const endpoint = this.endpoints.pop()
      if (endpoint) {
        resolve(endpoint)
      } else {
        this.options.getEndpoint(this.fileId)
          .then(endpoint => resolve(endpoint))
      }
    })
  }

  _endpointRelease (endpoint) {
    this.endpoints.push(endpoint)
  }

  start () {
    this.isPaused = false
    if (this.uploadId) {
      this._resumeUpload()
    } else {
      this._createUpload()
    }
  }

  pause () {
    const inProgress = this.uploading.slice()
    inProgress.forEach((xhr) => {
      xhr.abort()
    })
    this.isPaused = true
  }

  abort (opts = {}) {
    const really = opts.really || false

    if (!really) return this.pause()

    this._abortUpload()
  }
}

module.exports = MultipartUploader
