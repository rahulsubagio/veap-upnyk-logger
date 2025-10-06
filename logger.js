require('dotenv').config();
const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');

// --- Konfigurasi (Ambil dari Environment Variables) ---
const MQTT_BROKER_URL = process.env.MQTT_URL || 'wss://veap-upnyk.id/mqtt';
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const MQTT_TOPIC = 'indoorHidroponic/sensors/data';
const SAVE_INTERVAL_MS = 60 * 1000;

// Validasi konfigurasi
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('[ERROR] SUPABASE_URL dan SUPABASE_SERVICE_KEY harus diatur dalam file .env!');
  process.exit(1);
}

// --- Inisialisasi ---
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const mqttClient = mqtt.connect(MQTT_BROKER_URL);

let dataBuffer = [];

console.log(`[INFO] Logger dimulai. Data akan disimpan setiap ${SAVE_INTERVAL_MS / 1000} detik.`);

// --- Logika Utama ---
const saveData = async () => {
  if (dataBuffer.length === 0) {
    console.log(`[INFO] Tidak ada data baru untuk disimpan pada interval ini.`);
    return;
  }

  const dataToProcess = [...dataBuffer];
  dataBuffer = [];

  console.log(`[SAVE] Memproses ${dataToProcess.length} data point...`);

  const aggregates = {};

  for (const dataPoint of dataToProcess) {
    for (const key in dataPoint) {
      if (typeof dataPoint[key] === 'number') {
        if (!aggregates[key]) {
          aggregates[key] = { total: 0, count: 0 };
        }
        aggregates[key].total += dataPoint[key];
        aggregates[key].count++;
      }
    }
  }

  const averagedData = {};
  for (const key in aggregates) {
    const average = aggregates[key].total / aggregates[key].count;
    
    averagedData[key] = parseFloat(average.toFixed(2));
  }

  console.log('[SAVE] Data rata-rata yang akan disimpan:', averagedData);
  const { error } = await supabase.from('logs_indoor_hydroponic').insert(averagedData);

  if (error) {
    console.error('[ERROR] Gagal menyimpan data rata-rata:', error);
    dataBuffer.push(...dataToProcess);
  } else {
    console.log('[SUCCESS] Data rata-rata berhasil disimpan!');
  }
};

// --- Koneksi dan Listener MQTT ---
mqttClient.on('connect', () => {
  console.log('[INFO] Berhasil terhubung ke MQTT Broker!');
  mqttClient.subscribe(MQTT_TOPIC, (err) => {
    if (!err) console.log(`[INFO] Berhasil subscribe ke topik: ${MQTT_TOPIC}`);
    else console.error('[ERROR] Gagal subscribe:', err);
  });
});

// Setiap ada pesan masuk, cukup masukkan ke dalam buffer
mqttClient.on('message', (topic, message) => {
  if (topic === MQTT_TOPIC) {
    try {
      const sensorData = JSON.parse(message.toString());
      dataBuffer.push(sensorData);
      console.log(`[RECEIVE] Menerima data, ukuran buffer sekarang: ${dataBuffer.length}`);
    } catch (e) {
      console.error('[ERROR] Gagal memproses pesan MQTT:', e);
    }
  }
});

mqttClient.on('error', (err) => console.error('[ERROR] Koneksi MQTT Error:', err));
mqttClient.on('close', () => console.log('[INFO] Koneksi MQTT terputus.'));

setInterval(saveData, SAVE_INTERVAL_MS);