/**
 * Dependency-free WebGL equirectangular video viewer.
 * Multiple instances can share one HTMLVideoElement, so Up and Down views
 * use a single WebRTC connection and a single browser video decoder.
 */
(function (global) {
  'use strict';

  var VS = [
    'attribute vec2 p;',
    'varying vec2 screen;',
    'void main(){ screen=p; gl_Position=vec4(p,0.0,1.0); }',
  ].join('\n');

  var FS = [
    'precision mediump float;',
    'uniform sampler2D frame;',
    'uniform float yaw;',
    'uniform float pitch;',
    'uniform float tanHalfFov;',
    'uniform float aspect;',
    'varying vec2 screen;',
    'const float PI=3.141592653589793;',
    'void main(){',
    '  vec3 d=normalize(vec3(screen.x*aspect*tanHalfFov, screen.y*tanHalfFov, 1.0));',
    '  float cp=cos(pitch), sp=sin(pitch);',
    '  d=vec3(d.x, d.y*cp-d.z*sp, d.y*sp+d.z*cp);',
    '  float cy=cos(yaw), sy=sin(yaw);',
    '  d=vec3(d.x*cy+d.z*sy, d.y, -d.x*sy+d.z*cy);',
    '  vec2 uv=vec2(atan(d.x,d.z)/(2.0*PI)+0.5, asin(clamp(d.y,-1.0,1.0))/PI+0.5);',
    '  gl_FragColor=texture2D(frame, vec2(uv.x, 1.0-uv.y));',
    '}',
  ].join('\n');

  function shader(gl, type, source) {
    var s = gl.createShader(type);
    gl.shaderSource(s, source);
    gl.compileShader(s);
    if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) {
      throw new Error(gl.getShaderInfoLog(s) || 'WebGL shader compile failed');
    }
    return s;
  }

  function program(gl) {
    var p = gl.createProgram();
    gl.attachShader(p, shader(gl, gl.VERTEX_SHADER, VS));
    gl.attachShader(p, shader(gl, gl.FRAGMENT_SHADER, FS));
    gl.linkProgram(p);
    if (!gl.getProgramParameter(p, gl.LINK_STATUS)) {
      throw new Error(gl.getProgramInfoLog(p) || 'WebGL program link failed');
    }
    return p;
  }

  function SphereVideoView(canvas, video, opts) {
    opts = opts || {};
    this.canvas = canvas;
    this.video = video;
    this.yaw = Number(opts.yaw || 0);
    this.pitch = Number(opts.pitch || 0);
    this.fov = Number(opts.fov || 80);
    this.drag = null;
    this.raf = 0;
    this.dead = false;

    var gl = canvas.getContext('webgl', {
      alpha: false,
      antialias: true,
      powerPreference: 'high-performance',
    });
    if (!gl) throw new Error('WebGL is not available in this browser');
    this.gl = gl;
    this.prog = program(gl);
    gl.useProgram(this.prog);

    var vertices = new Float32Array([-1,-1, 1,-1, -1,1, -1,1, 1,-1, 1,1]);
    var buf = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, buf);
    gl.bufferData(gl.ARRAY_BUFFER, vertices, gl.STATIC_DRAW);
    var pos = gl.getAttribLocation(this.prog, 'p');
    gl.enableVertexAttribArray(pos);
    gl.vertexAttribPointer(pos, 2, gl.FLOAT, false, 0, 0);

    this.texture = gl.createTexture();
    gl.bindTexture(gl.TEXTURE_2D, this.texture);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.REPEAT);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, 1, 1, 0, gl.RGBA, gl.UNSIGNED_BYTE,
      new Uint8Array([0, 0, 0, 255]));

    this.uYaw = gl.getUniformLocation(this.prog, 'yaw');
    this.uPitch = gl.getUniformLocation(this.prog, 'pitch');
    this.uFov = gl.getUniformLocation(this.prog, 'tanHalfFov');
    this.uAspect = gl.getUniformLocation(this.prog, 'aspect');
    gl.uniform1i(gl.getUniformLocation(this.prog, 'frame'), 0);

    this._bindInput();
    this._render = this._render.bind(this);
    this.raf = requestAnimationFrame(this._render);
  }

  SphereVideoView.prototype._bindInput = function () {
    var self = this;
    this.canvas.addEventListener('pointerdown', function (ev) {
      self.drag = { x: ev.clientX, y: ev.clientY, yaw: self.yaw, pitch: self.pitch };
      self.canvas.setPointerCapture(ev.pointerId);
    });
    this.canvas.addEventListener('pointermove', function (ev) {
      if (!self.drag) return;
      self.yaw = self.drag.yaw - (ev.clientX - self.drag.x) * 0.005;
      self.pitch = Math.max(-1.45, Math.min(1.45,
        self.drag.pitch + (ev.clientY - self.drag.y) * 0.005));
    });
    function end() { self.drag = null; }
    this.canvas.addEventListener('pointerup', end);
    this.canvas.addEventListener('pointercancel', end);
    this.canvas.addEventListener('wheel', function (ev) {
      ev.preventDefault();
      self.fov = Math.max(35, Math.min(120, self.fov + ev.deltaY * 0.04));
    }, { passive: false });
    this.canvas.addEventListener('dblclick', function () {
      var el = self.canvas.parentElement || self.canvas;
      if (el.requestFullscreen) el.requestFullscreen();
    });
  };

  SphereVideoView.prototype._resize = function () {
    var dpr = Math.min(global.devicePixelRatio || 1, 2);
    var w = Math.max(2, Math.round(this.canvas.clientWidth * dpr));
    var h = Math.max(2, Math.round(this.canvas.clientHeight * dpr));
    if (this.canvas.width !== w || this.canvas.height !== h) {
      this.canvas.width = w;
      this.canvas.height = h;
    }
    this.gl.viewport(0, 0, w, h);
  };

  SphereVideoView.prototype._render = function () {
    if (this.dead) return;
    var gl = this.gl;
    this._resize();
    if (this.video.readyState >= 2 && this.video.videoWidth) {
      try {
        gl.bindTexture(gl.TEXTURE_2D, this.texture);
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, this.video);
      } catch (e) {
        // A transient frame upload failure is safe; retry next animation frame.
      }
    }
    gl.useProgram(this.prog);
    gl.uniform1f(this.uYaw, this.yaw);
    gl.uniform1f(this.uPitch, this.pitch);
    gl.uniform1f(this.uFov, Math.tan(this.fov * Math.PI / 360));
    gl.uniform1f(this.uAspect, this.canvas.width / this.canvas.height);
    gl.drawArrays(gl.TRIANGLES, 0, 6);
    this.raf = requestAnimationFrame(this._render);
  };

  SphereVideoView.prototype.destroy = function () {
    this.dead = true;
    if (this.raf) cancelAnimationFrame(this.raf);
  };

  global.QooCam360 = {
    create: function (canvas, video, opts) {
      return new SphereVideoView(canvas, video, opts);
    },
  };
})(typeof window !== 'undefined' ? window : this);
