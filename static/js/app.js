document.addEventListener('DOMContentLoaded', () => {
    const map = new mapboxgl.Map({
        container: 'map',
        style: 'mapbox://styles/mapbox/light-v10',
        center: [0, 20],
        zoom: 1.5
    });

    function showNotification(msg, type='info') {
        alert(msg);
    }

    function updateProgress(pct, text) {
        document.getElementById('progressBar').style.width = pct + '%';
        document.getElementById('progressText').textContent = text;
    }

    function resetUploadUI() {
        document.getElementById('progressContainer').style.display = 'none';
        document.getElementById('uploadBtn').style.display = 'none';
        document.getElementById('fileInput').value = '';
    }

    function displaySingleResult(data) {
        // existing single-file display logic...
    }

    function handleFileSelect(event) {
        const files = Array.from(event.target.files);
        if (files.length && files.every(f => f.name.endsWith('.nc'))) {
            document.getElementById('uploadBtn').style.display = 'block';
            document.getElementById('vizTypeSelection').style.display = 'block';
            if (files.length === 1) {
                document.getElementById('uploadBtn').textContent = `⬆️ Upload ${files[0].name}`;
            } else {
                document.getElementById('uploadBtn').textContent = `⬆️ Upload ${files.length} files`;
            }
        } else {
            showNotification('Please select valid NetCDF (.nc) files', 'error');
            event.target.value = '';
        }
    }

    async function uploadFile() {
        const fileInput = document.getElementById('fileInput');
        const files = Array.from(fileInput.files);
        if (!files.length) {
            showNotification('Please select files first', 'error');
            return;
        }

        const vizType = document.querySelector('input[name="vizType"]:checked').value;
        const formData = new FormData();
        formData.append('create_tileset', 'true');
        formData.append('visualization_type', vizType);

        let endpoint = '/api/upload-netcdf';
        if (files.length > 1) {
            endpoint = '/api/upload-netcdf-batch';
            files.forEach(f => formData.append('files', f));
        } else {
            formData.append('file', files[0]);
        }

        document.getElementById('progressContainer').style.display = 'block';
        document.getElementById('uploadBtn').style.display = 'none';
        updateProgress(10, 'Uploading file(s)...');

        try {
            const response = await fetch(endpoint, { method: 'POST', body: formData });
            if (!response.ok) {
                const err = await response.text();
                showNotification('Upload failed: ' + err, 'error');
                resetUploadUI();
                return;
            }
            const data = await response.json();
            if (files.length > 1) {
                console.log('Batch upload result:', data);
            } else {
                displaySingleResult(data);
            }
        } catch (error) {
            showNotification('Upload failed: ' + error.message, 'error');
            resetUploadUI();
        }
    }

    document.getElementById('fileInput').addEventListener('change', handleFileSelect);
    document.getElementById('uploadBtn').addEventListener('click', uploadFile);
});
