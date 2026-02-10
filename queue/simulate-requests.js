const axios = require('axios');
const fs = require('fs');
const path = require('path');
const FormData = require('form-data');

const filePath = path.join(__dirname,'storage', '1770294971612-test-1.txt');
const fileBuffer = fs.readFileSync(filePath);

const ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzA3MjMzMzgsImlkIjoiODI0Y2U0OWEtYzAyNy00NmY3LTk4MTQtODkzNGRhOGU3NjFiIiwidXNlckRhdGEiOnsiaWQiOiI4MjRjZTQ5YS1jMDI3LTQ2ZjctOTgxNC04OTM0ZGE4ZTc2MWIiLCJ1c2VyTmFtZSI6InNvcGhpYS5zdXBlcmFkbWluQGV4cGxlb2dyb3VwLmNvbSIsInBhc3N3b3JkIjoiVTJGc2RHVmtYMTlKTjdibVpKb0Q2VlpMY1R5MUJzZjR1UWg3QmVHZnQ1N3VxME5lV3h6dCtEMU45TGt0NTV2QiIsImZpcnN0TmFtZSI6IlNvcGhpYSIsImxhc3ROYW1lIjoiU3VwZXIgQWRtaW4iLCJyb2xlIjoiU3VwZXIgQWRtaW4iLCJlbWFpbCI6InNvcGhpYS5zdXBlcmFkbWluQGV4cGxlb2dyb3VwLmNvbSIsInRpbWVzdGFtcCI6IjE3NzAwMTMyMjYwMDAiLCJtb2RlbCI6ImRlZmF1bHRfbW9kZWwiLCJpc0FjdGl2ZSI6dHJ1ZSwiaXNMb2dpblJlcXVlc3RBY3RpdmUiOmZhbHNlLCJwaXBlbGluZVN0ZXBzIjpudWxsLCJpc19kZWxldGVkIjpmYWxzZX0sImlhdCI6MTc3MDcyMTUzOH0.ZQLttKkC013D01AnaZipNIQX5_MOn1I-DnL8GmOe7DQ";
const PROJECT_ID = "13c47b86-26d4-4429-b603-e8ecbd4c77d4";
const USER_ID = "824ce49a-c027-46f7-9814-8934da8e761b";



async function simulateRequests() {
  const requests = [];

  // for (let i = 0; i < 10; i++) {
  //   const data = new FormData();
  //   data.append('file', fileBuffer, `test-${i}.txt`);
  //    data.append('file_name', `Media-${i + 1}`);
  //   data.append('process_image', false);
  //   data.append('isTestcase', false);
  //   data.append(
  //     'nodes',
  //     JSON.stringify(["d41c99e2-0e4b-4e13-9ca8-eb077d02dd90"])
  //   );

  //   requests.push(
  //     axios.post('http://localhost:8502/api/upload_file', data, {
  //       headers: data.getHeaders(),
  //         "X-Access-Token": ACCESS_TOKEN,
  //           "X-Project-Id": PROJECT_ID,
  //           "X-User-Id": USER_ID,
  //     })
  //   );
  // }

  // const responses = await Promise.all(requests);

  // responses.forEach((res, index) => {
  //   console.log(`User ${index + 1} → JobId: ${res.data.jobId}`);
  // });

// const requests = [];

  for (let i = 0; i < 10; i++) {
    const data = new FormData();
    // Use the buffer directly
    data.append('file', fileBuffer, { filename: `test-${i}.txt` });
    data.append('file_name', `Media-${i + 1}`);
    data.append('process_image', 'false'); // Form-data prefers strings
    data.append('isTestcase', 'false');
    data.append(
      'nodes',
      JSON.stringify(["d41c99e2-0e4b-4e13-9ca8-eb077d02dd90"])
    );

    requests.push(
      axios.post('http://localhost:8502/api/upload_file', data, {
        headers: {
          ...data.getHeaders(), // This includes the 'content-type' with boundary
          "X-Access-Token": ACCESS_TOKEN,
          "X-Project-Id": PROJECT_ID,
          "X-User-Id": USER_ID,
        },
      })
    );
  }

  try {
    const responses = await Promise.all(requests);
    responses.forEach((res, index) => {
      console.log(`User ${index + 1} → JobId: ${res.data.jobId || 'No Job ID returned'}`);
    });
  } catch (error) {
    // Better error logging for Axios
    console.error("Request Failed:", error.response ? error.response.data : error.message);
  }


}

simulateRequests().catch(console.error);
