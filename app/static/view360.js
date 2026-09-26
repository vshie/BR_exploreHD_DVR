/**
 * One square per QooCam lens from a 3840x1920 equirectangular frame.
 * The stitch seams sit on the quarter lines. Each lens is the 1920-wide
 * span centered between those seams. The second lens wraps the frame edge.
 * Both views share one video element. No scaling on the Pi.
 */
(function (global) {
  'use strict';

  function LensView(canvas, video, lens) {
    this.canvas = canvas;
    this.video = video;
    this.lens = lens === 'center' ? 'center' : 'edge';
    this.dead = false;
    this.ctx = canvas.getContext('2d');
    this._draw = this._draw.bind(this);
    this.raf = requestAnimationFrame(this._draw);
  }

  LensView.prototype._draw = function () {
    if (this.dead) return;
    var canvas = this.canvas;
    var video = this.video;
    var dpr = Math.min(global.devicePixelRatio || 1, 2);
    var size = Math.max(2, Math.round(Math.min(canvas.clientWidth, canvas.clientHeight) * dpr));
    if (canvas.width !== size || canvas.height !== size) {
      canvas.width = size;
      canvas.height = size;
    }
    var ctx = this.ctx;
    if (!(video.readyState >= 2 && video.videoWidth && video.videoHeight)) {
      // Keep the last painted frame. Clearing here flashes black on every
      // brief buffer gap, which is what a live-edge stall looks like.
      this.raf = requestAnimationFrame(this._draw);
      return;
    }
    var width = video.videoWidth;
    var height = video.videoHeight;
    var quarter = width / 4;
    var half = width / 2;
    if (this.lens === 'center') {
      ctx.drawImage(video, quarter, 0, half, height, 0, 0, size, size);
    } else {
      ctx.drawImage(video, quarter * 3, 0, quarter, height, 0, 0, size / 2, size);
      ctx.drawImage(video, 0, 0, quarter, height, size / 2, 0, size / 2, size);
    }
    this.raf = requestAnimationFrame(this._draw);
  };

  LensView.prototype.destroy = function () {
    this.dead = true;
    if (this.raf) cancelAnimationFrame(this.raf);
  };

  global.QooCam360 = {
    create: function (canvas, video, opts) {
      return new LensView(canvas, video, opts && opts.lens);
    },
  };
})(typeof window !== 'undefined' ? window : this);
