/**
 * Small WHEP reader for the local MediaMTX bridge.
 *
 * It waits for ICE gathering before POSTing the offer, avoiding a separate
 * trickle-ICE PATCH implementation on this LAN-only connection.
 */
(function (global) {
  'use strict';

  function waitForIce(pc, timeoutMs) {
    if (pc.iceGatheringState === 'complete') return Promise.resolve();
    return new Promise(function (resolve) {
      var done = false;
      var timer = setTimeout(finish, timeoutMs || 4000);
      function finish() {
        if (done) return;
        done = true;
        clearTimeout(timer);
        pc.removeEventListener('icegatheringstatechange', changed);
        resolve();
      }
      function changed() {
        if (pc.iceGatheringState === 'complete') finish();
      }
      pc.addEventListener('icegatheringstatechange', changed);
    });
  }

  function WhepReader(url, hooks) {
    this.url = url;
    this.hooks = hooks || {};
    this.pc = null;
    this.sessionUrl = null;
    this.closed = false;
  }

  WhepReader.prototype.connect = async function () {
    var self = this;
    self.closed = false;
    self.pc = new RTCPeerConnection({
      iceServers: [],
      bundlePolicy: 'max-bundle',
    });
    self.pc.addTransceiver('video', { direction: 'recvonly' });
    self.pc.ontrack = function (ev) {
      if (ev.track.kind === 'video' && self.hooks.onTrack) {
        self.hooks.onTrack(ev);
      }
    };
    self.pc.onconnectionstatechange = function () {
      var state = self.pc ? self.pc.connectionState : 'closed';
      if (self.hooks.onStatus) self.hooks.onStatus('WebRTC: ' + state);
      if ((state === 'failed' || state === 'disconnected') && self.hooks.onError) {
        self.hooks.onError('WebRTC ' + state);
      }
    };

    if (self.hooks.onStatus) self.hooks.onStatus('Creating WebRTC offer…');
    var offer = await self.pc.createOffer();
    await self.pc.setLocalDescription(offer);
    await waitForIce(self.pc, 5000);
    if (self.closed) throw new Error('preview closed');

    if (self.hooks.onStatus) self.hooks.onStatus('Opening local QooCam preview…');
    var response = await fetch(self.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/sdp' },
      body: self.pc.localDescription.sdp,
    });
    if (response.status !== 201) {
      var detail = await response.text();
      throw new Error('WHEP ' + response.status + (detail ? ': ' + detail : ''));
    }
    self.sessionUrl = new URL(response.headers.get('Location'), self.url).toString();
    var answer = await response.text();
    await self.pc.setRemoteDescription({ type: 'answer', sdp: answer });
  };

  WhepReader.prototype.close = function () {
    this.closed = true;
    if (this.sessionUrl) {
      fetch(this.sessionUrl, { method: 'DELETE' }).catch(function () {});
      this.sessionUrl = null;
    }
    if (this.pc) {
      try { this.pc.close(); } catch (e) {}
      this.pc = null;
    }
  };

  global.QooCamWhep = {
    create: function (url, hooks) { return new WhepReader(url, hooks); },
  };
})(typeof window !== 'undefined' ? window : this);
