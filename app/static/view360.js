/**
 * Fixed Up / Down crops of one equirectangular QooCam frame.
 * Both views share one video element. No WebGL and no camera control.
 */
(function (global) {
  'use strict';

  function HalfView(canvas, video, half) {
    this.canvas = canvas;
    this.video = video;
    this.half = half === 'bottom' ? 'bottom' : 'top';
    this.dead = false;
    this.ctx = canvas.getContext('2d');
    this._draw = this._draw.bind(this);
    this.raf = requestAnimationFrame(this._draw);
  }

  HalfView.prototype._draw = function () {
    if (this.dead) return;
    var canvas = this.canvas;
    var video = this.video;
    var dpr = Math.min(global.devicePixelRatio || 1, 2);
    var w = Math.max(2, Math.round(canvas.clientWidth * dpr));
    var h = Math.max(2, Math.round(canvas.clientHeight * dpr));
    if (canvas.width !== w || canvas.height !== h) {
      canvas.width = w;
      canvas.height = h;
    }
    var ctx = this.ctx;
    if (video.readyState >= 2 && video.videoWidth && video.videoHeight) {
      var srcH = video.videoHeight / 2;
      var srcY = this.half === 'bottom' ? srcH : 0;
      ctx.drawImage(video, 0, srcY, video.videoWidth, srcH, 0, 0, w, h);
    } else {
      ctx.fillStyle = '#000';
      ctx.fillRect(0, 0, w, h);
    }
    this.raf = requestAnimationFrame(this._draw);
  };

  HalfView.prototype.destroy = function () {
    this.dead = true;
    if (this.raf) cancelAnimationFrame(this.raf);
  };

  global.QooCam360 = {
    create: function (canvas, video, opts) {
      return new HalfView(canvas, video, opts && opts.half);
    },
  };
})(typeof window !== 'undefined' ? window : this);
