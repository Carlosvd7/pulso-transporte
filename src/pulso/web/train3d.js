/* Tren 3D decorativo de la cabecera (three.js r128, modelado por código: sin modelos externos).
 * Un tren de cercanías estilizado de tres coches avanza sobre una vía con traviesas en movimiento.
 * - Colores tomados de las variables CSS (se adapta a claro/oscuro).
 * - Estático si el sistema pide reducir animaciones; se pausa fuera de pantalla o con la pestaña oculta.
 */
(() => {
  "use strict";
  const host = document.getElementById("hero-train");
  if (!host || typeof THREE === "undefined") return;
  const probe = document.createElement("canvas");
  if (!(probe.getContext("webgl") || probe.getContext("experimental-webgl"))) { host.hidden = true; return; }

  const css = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim() || "#888";
  const reduced = matchMedia("(prefers-reduced-motion: reduce)");

  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.outputEncoding = THREE.sRGBEncoding;
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  host.appendChild(renderer.domElement);
  renderer.domElement.setAttribute("aria-hidden", "true");

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(24, 1, 0.1, 200);
  camera.position.set(7, 3.6, 17.5);
  camera.lookAt(-0.6, 0.6, 0);

  // ---------------------------------------------------------------- materiales
  const mat = {
    body: new THREE.MeshStandardMaterial({ roughness: 0.35, metalness: 0.15 }),
    accent: new THREE.MeshStandardMaterial({ roughness: 0.4, metalness: 0.1 }),
    glass: new THREE.MeshStandardMaterial({ roughness: 0.15, metalness: 0.6 }),
    dark: new THREE.MeshStandardMaterial({ roughness: 0.8, metalness: 0.2 }),
    rail: new THREE.MeshStandardMaterial({ roughness: 0.3, metalness: 0.9 }),
    sleeper: new THREE.MeshStandardMaterial({ roughness: 0.9 }),
    ground: new THREE.ShadowMaterial({ opacity: 0.12 }),
    light: new THREE.MeshBasicMaterial({ color: 0xfff3c4 }),
  };

  function applyTheme() {
    const dark = document.documentElement.dataset.theme
      ? document.documentElement.dataset.theme === "dark"
      : matchMedia("(prefers-color-scheme: dark)").matches;
    mat.body.color.set(dark ? "#e9e8e4" : "#fbfbfa");
    mat.accent.color.set(css("--accent"));
    mat.glass.color.set(dark ? "#0f1b2a" : "#1d2f45");
    mat.dark.color.set(dark ? "#2a2a28" : "#3a3a38");
    mat.rail.color.set(dark ? "#8c8b85" : "#6f6e69");
    mat.sleeper.color.set(dark ? "#3b3a36" : "#aaa79d");
    mat.ground.opacity = dark ? 0.35 : 0.2;
  }

  // -------------------------------------------------------------------- luces
  scene.add(new THREE.HemisphereLight(0xffffff, 0x8a8a80, 0.6));
  const sun = new THREE.DirectionalLight(0xffffff, 1.0);
  sun.position.set(6, 12, 8);
  sun.castShadow = true;
  sun.shadow.mapSize.set(1024, 1024);
  Object.assign(sun.shadow.camera, { left: -14, right: 14, top: 6, bottom: -6, near: 1, far: 40 });
  scene.add(sun);

  // ------------------------------------------------------------- coche del tren
  const CAR_LEN = 5.2, CAR_W = 1.25, CAR_H = 1.45, GAP = 0.18;

  function carBody(withNose) {
    // Perfil lateral (x a lo largo, y en altura) extruido a lo ancho; morro inclinado en cabeza.
    const s = new THREE.Shape();
    const L = CAR_LEN, H = CAR_H, r = 0.22;
    s.moveTo(0, 0.12);
    s.lineTo(L - (withNose ? 0.2 : 0), 0.12);
    if (withNose) {
      s.quadraticCurveTo(L + 0.55, 0.2, L + 0.62, 0.62);
      s.quadraticCurveTo(L + 0.5, H - 0.1, L - 0.75, H);
    } else {
      s.lineTo(L, H - r);
      s.quadraticCurveTo(L, H, L - r, H);
    }
    s.lineTo(r, H);
    s.quadraticCurveTo(0, H, 0, H - r);
    s.lineTo(0, 0.12);
    const g = new THREE.ExtrudeGeometry(s, { depth: CAR_W, bevelEnabled: true, bevelThickness: 0.06, bevelSize: 0.06, bevelSegments: 3, curveSegments: 16 });
    g.translate(0, 0, -CAR_W / 2);
    const m = new THREE.Mesh(g, mat.body);
    m.castShadow = true;
    return m;
  }

  function car(withNose) {
    const grp = new THREE.Group();
    grp.add(carBody(withNose));
    // faldón inferior oscuro: separa la caja de la vía y da contraste sobre fondo claro
    const skirt = new THREE.Mesh(new THREE.BoxGeometry(CAR_LEN - 0.25, 0.16, CAR_W + 0.17), mat.dark);
    skirt.position.set(CAR_LEN / 2, 0.2, 0);
    skirt.castShadow = true;
    grp.add(skirt);
    // franja de color de la línea
    const stripe = new THREE.Mesh(new THREE.BoxGeometry(CAR_LEN - 0.3, 0.16, CAR_W + 0.14), mat.accent);
    stripe.position.set(CAR_LEN / 2, 0.42, 0);
    grp.add(stripe);
    // ventanas
    const win = new THREE.Mesh(new THREE.BoxGeometry(CAR_LEN - 1.4, 0.42, CAR_W + 0.13), mat.glass);
    win.position.set(CAR_LEN / 2 - 0.15, 0.98, 0);
    grp.add(win);
    // puertas (dos por lado)
    for (const x of [1.15, CAR_LEN - 1.45]) {
      const door = new THREE.Mesh(new THREE.BoxGeometry(0.55, 0.95, CAR_W + 0.15), mat.dark);
      door.position.set(x, 0.72, 0);
      grp.add(door);
      const dw = new THREE.Mesh(new THREE.BoxGeometry(0.4, 0.34, CAR_W + 0.16), mat.glass);
      dw.position.set(x, 0.98, 0);
      grp.add(dw);
    }
    if (withNose) {
      const shield = new THREE.Mesh(new THREE.BoxGeometry(0.5, 0.42, CAR_W - 0.05), mat.glass);
      shield.position.set(CAR_LEN + 0.12, 1.02, 0);
      shield.rotation.z = -0.62;
      grp.add(shield);
      for (const z of [-0.38, 0.38]) {
        const lamp = new THREE.Mesh(new THREE.SphereGeometry(0.07, 12, 8), mat.light);
        lamp.position.set(CAR_LEN + 0.52, 0.5, z);
        grp.add(lamp);
      }
    }
    // bogies y ruedas
    for (const bx of [0.9, CAR_LEN - 0.9]) {
      const bogie = new THREE.Mesh(new THREE.BoxGeometry(1.2, 0.18, CAR_W - 0.2), mat.dark);
      bogie.position.set(bx, 0.12, 0);
      grp.add(bogie);
      for (const dx of [-0.38, 0.38]) {
        for (const z of [-0.5, 0.5]) {
          const w = new THREE.Mesh(new THREE.CylinderGeometry(0.17, 0.17, 0.08, 18), mat.dark);
          w.rotation.x = Math.PI / 2;
          w.position.set(bx + dx, 0.05, z);
          grp.add(w);
        }
      }
    }
    // pantógrafo en el coche de cabeza
    if (withNose) {
      const base = new THREE.Mesh(new THREE.BoxGeometry(0.6, 0.06, 0.5), mat.dark);
      base.position.set(CAR_LEN / 2, CAR_H + 0.05, 0);
      grp.add(base);
      const arm = new THREE.Mesh(new THREE.BoxGeometry(0.04, 0.55, 0.04), mat.dark);
      arm.position.set(CAR_LEN / 2 - 0.12, CAR_H + 0.3, 0);
      arm.rotation.z = 0.55;
      grp.add(arm);
      const head = new THREE.Mesh(new THREE.BoxGeometry(0.06, 0.04, 0.9), mat.dark);
      head.position.set(CAR_LEN / 2 + 0.05, CAR_H + 0.55, 0);
      grp.add(head);
    }
    return grp;
  }

  // Composición: coche de cabeza, coche intermedio y coche de cola (con morro hacia atrás).
  const train = new THREE.Group();
  const head = car(true);
  const middle = car(false);
  middle.position.x = -(CAR_LEN + GAP);
  const tail = car(true);
  tail.rotation.y = Math.PI;
  tail.position.x = -2 * (CAR_LEN + GAP) + CAR_LEN;
  train.add(head, middle, tail);
  train.position.set(1.5, 0.18, 0);
  scene.add(train);

  // ---------------------------------------------------------------------- vía
  const track = new THREE.Group();
  for (const z of [-0.52, 0.52]) {
    const rail = new THREE.Mesh(new THREE.BoxGeometry(60, 0.08, 0.08), mat.rail);
    rail.position.set(-8, 0.13, z);
    rail.receiveShadow = true;
    track.add(rail);
  }
  const SLEEPER_STEP = 0.7, N_SLEEPERS = 80;
  const sleeperGeo = new THREE.BoxGeometry(0.22, 0.08, 1.7);
  const sleepers = new THREE.InstancedMesh(sleeperGeo, mat.sleeper, N_SLEEPERS);
  sleepers.receiveShadow = true;
  track.add(sleepers);
  const ground = new THREE.Mesh(new THREE.PlaneGeometry(80, 30), mat.ground);
  ground.rotation.x = -Math.PI / 2;
  ground.receiveShadow = true;
  track.add(ground);
  scene.add(track);

  const dummy = new THREE.Object3D();
  function placeSleepers(offset) {
    for (let i = 0; i < N_SLEEPERS; i++) {
      const x = 12 - ((i * SLEEPER_STEP + offset) % (N_SLEEPERS * SLEEPER_STEP));
      dummy.position.set(x, 0.05, 0);
      dummy.updateMatrix();
      sleepers.setMatrixAt(i, dummy.matrix);
    }
    sleepers.instanceMatrix.needsUpdate = true;
  }

  // ------------------------------------------------------------------- tamaño
  function resize() {
    const w = host.clientWidth, h = host.clientHeight;
    if (!w || !h) return;
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
  }
  new ResizeObserver(resize).observe(host);

  // ---------------------------------------------------------------- animación
  let visible = true, last = performance.now(), t = 0, raf = 0;
  const SPEED = 2.4; // unidades de escena por segundo
  function frame(now) {
    raf = 0;
    const dt = Math.min(0.05, (now - last) / 1000);
    last = now;
    if (!reduced.matches) t += dt;
    placeSleepers(t * SPEED);
    // leve vaivén para dar sensación de marcha
    train.position.y = 0.18 + Math.sin(t * 9) * 0.008;
    train.rotation.x = Math.sin(t * 5.3) * 0.004;
    renderer.render(scene, camera);
    if (visible && !document.hidden && !reduced.matches) raf = requestAnimationFrame(frame);
  }
  function kick() { if (!raf) { last = performance.now(); raf = requestAnimationFrame(frame); } }

  new IntersectionObserver((entries) => {
    visible = entries.some((e) => e.isIntersecting);
    if (visible) kick();
  }).observe(host);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) kick(); });
  reduced.addEventListener("change", kick);

  const retheme = () => { applyTheme(); kick(); };
  new MutationObserver(retheme).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", retheme);

  applyTheme();
  resize();
  kick();
})();
