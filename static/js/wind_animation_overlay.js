class WindAnimationOverlay {
  constructor(windData, options = {}) {
    this.id = 'wind-animation-overlay';
    this.type = 'custom';
    this.renderingMode = '2d';
    this.windData = windData;        // { width, height, u: [], v: [] }
    this.particles = [];
    this.maxParticles = options.maxParticles || 1000;
    this.particleSpeed = options.particleSpeed || 0.01;
    this.frame = null;
  }

  onAdd(map, gl) {
    this.map = map;
    this.canvas = document.createElement('canvas');
    this.ctx = this.canvas.getContext('2d');
    this._resizeCanvas();
    this.canvas.style.position = 'absolute';
    this.canvas.style.top = 0;
    this.canvas.style.left = 0;
    map.getCanvasContainer().appendChild(this.canvas);
    this._initParticles();
    this._animate();
    map.on('move', () => this._resizeCanvas());
  }

  _resizeCanvas() {
    const container = this.map.getCanvas();
    this.canvas.width = container.clientWidth;
    this.canvas.height = container.clientHeight;
  }

  _initParticles() {
    const w = this.canvas.width, h = this.canvas.height;
    for (let i = 0; i < this.maxParticles; i++) {
      this.particles.push({
        x: Math.random() * w,
        y: Math.random() * h,
        age: Math.random() * 100
      });
    }
  }

  _getWindVector(x, y) {
    // map canvas coords to windData grid
    const i = Math.floor(y / this.canvas.height * this.windData.height);
    const j = Math.floor(x / this.canvas.width * this.windData.width);
    const idx = i * this.windData.width + j;
    return {
      u: this.windData.u[idx] || 0,
      v: this.windData.v[idx] || 0
    };
  }

  _animate() {
    const ctx = this.ctx;
    // fade existing trails
    ctx.globalCompositeOperation = 'destination-in';
    ctx.fillStyle = 'rgba(0, 0, 0, 0.97)';
    ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);
    // draw new particles
    ctx.globalCompositeOperation = 'lighter';
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.8)';
    ctx.lineWidth = 1;

    for (const p of this.particles) {
      const v = this._getWindVector(p.x, p.y);
      const newX = p.x + v.u * this.particleSpeed;
      const newY = p.y + v.v * this.particleSpeed;
      ctx.beginPath();
      ctx.moveTo(p.x, p.y);
      ctx.lineTo(newX, newY);
      ctx.stroke();
      p.x = newX;
      p.y = newY;
      p.age++;
      if (p.x < 0 || p.x > this.canvas.width || p.y < 0 || p.y > this.canvas.height || p.age > 100) {
        p.x = Math.random() * this.canvas.width;
        p.y = Math.random() * this.canvas.height;
        p.age = 0;
      }
    }

    this.frame = requestAnimationFrame(() => this._animate());
  }

  render(gl, matrix) {
    // required stub for Mapbox custom layer
  }

  onRemove() {
    cancelAnimationFrame(this.frame);
    this.canvas.parentNode.removeChild(this.canvas);
  }
}

window.WindAnimationOverlay = WindAnimationOverlay;
