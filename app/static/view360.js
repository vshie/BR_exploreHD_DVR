/**
 * Left and right 1920x1920 crops of one 3840x1920 QooCam frame.
 * Both views share one video element. No scaling on the Pi.
 */
(function (global) {
  'use strict';

  function SquareHalf(canvas, video, side) {
    this.canvas = canvas;
    this.video = video;
    this.side = side === 'right' ? 'right' : 'left';
    this.dead = false;
    this.ctx = canvas.getContext('2d');
    this._draw = this._draw.bind(this);
    this.raf = requestAnimationFrame(this._draw);
  }

  SquareHalf.prototype._draw = function () {
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
    if (video.readyState >= 2 && video.videoWidth && video.videoHeight) {
      var srcW = video.videoWidth / 2;
      var srcX = this.side === 'right' ? srcW : 0;
      ctx.drawImage(video, srcX, 0, srcW, video.videoHeight, 0, 0, size, size);
    } else {
      ctx.fillStyle = '#000';
      ctx.fillRect(0, 0, size, size);
    }
    this.raf = requestAnimationFrame(this._draw);
  };

  SquareHalf.prototype.destroy = function () {
    this.dead = true;
    if (this.raf) cancelAnimationFrame(this.raf);
  };

  global.QooCam360 = {
    create: function (canvas, video, opts) {
      return new SquareHalf(canvas, video, opts && opts.side);
    },
  };
})(typeof window !== 'undefined' ? window : this);
