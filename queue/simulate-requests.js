const axios = require('axios');
const fs = require('fs');
const path = require('path');
const FormData = require('form-data');

const filePath = path.join(__dirname, 'storage', '1770294971612-test-1.txt');
const fileBuffer = fs.readFileSync(filePath);

/* --------------------------------------------------
   Define job durations (ms)
--------------------------------------------------- */
const DURATIONS = [
  600000, // Job 1 → 10 min
  120000, // Job 2 → 2 min
  60000,  // Job 3 → 1 min
  180000, // Job 4 → 3 min
  60000,  // Job 5 → 1 min
  60000,
  60000,
  60000,
  60000,
  60000,
];

async function simulateRequests() {
  const requests = [];

  for (let i = 0; i < 10; i++) {
    const duration = DURATIONS[i] || 60000;

    const formData = new FormData();
    formData.append('file', fileBuffer, `test-${i + 10}.txt`);

    console.log(
      `📤 Sending Job-${i + 1} (duration=${duration / 1000}s)`
    );

    requests.push(
      axios.post(
        `http://localhost:3001/upload?duration=${duration}&jobId=job-${i + 1}`,
        formData,
        {
          headers: formData.getHeaders(),
        }
      )
    );
  }

  const responses = await Promise.all(requests);

  responses.forEach((res, index) => {
    console.log(
      `✅ User ${index + 1} → JobId: ${res.data.jobId}, duration=${DURATIONS[index] / 1000}s`
    );
  });
}

simulateRequests().catch((err) => {
  console.error('❌ Simulation failed:', err.message);
});
