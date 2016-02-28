# Space Shepherd

Backend: Python (Flask), MySQL  
Frontend: JavaScript (D3)

Self-signed certificates generated via instructions [here](http://www.akadia.com/services/ssh_test_certificate.html)


![Baxter's Dropbox](/screenshots/baxter.jpg)

Post-submission changes:

- On client, do long polling to wait for changes instead of blindly polling delta() every 3 seconds
- On client, fade in treemap upon initial load
- On backend, prefer processing delta()s in memory when the number of delta entries is high
