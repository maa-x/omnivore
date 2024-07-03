import express from 'express';
import cors from 'cors'
import { createHmac } from 'crypto';
import { storageService } from './storage_service';
import { corsConfig } from './cors_config';

const SECRET_KEY = process.env.FS_UPLOAD_SECRET_KEY

function verifySignature(payload: string, providedSignature: string): boolean {
    if (!SECRET_KEY) {
        throw new Error('Secret key is not set');
    }
    const expectedSignature = createHmac('sha256', SECRET_KEY).update(payload).digest('hex');
    return providedSignature === expectedSignature;
}

export function storageServiceRouter() {
    const router = express.Router();

    router.options('/upload', cors<express.Request>(corsConfig));
    router.put('/upload', cors<express.Request>(corsConfig), express.raw({ type: '*/*', limit: '8mb' }), (req, res) => {
        const { filename, expiry, signature } = req.query;
        const contentType = req.query.contentType?.toString() || req.headers['content-type']?.toString() || 'application/octet-stream';

        if (!filename || !expiry || !signature) {
            return res.status(400).send('Missing required parameters');
        }

        const payload = `${filename}:${expiry}:${contentType}`;
        const currentTime = Math.floor(Date.now() / 1000);

        if (!verifySignature(payload, signature.toString()) || currentTime > parseInt(expiry.toString(), 10)) {
            return res.status(403).send('Invalid or expired signature');
        }

        storageService.save(filename.toString(), req.body, { contentType: contentType, timeout: 30000 })

        res.send('File uploaded successfully');
    });

    router.options('/download', cors<express.Request>(corsConfig));
    router.get('/download', cors<express.Request>(corsConfig), (req, res) => {
        const { filename, expiry, signature } = req.query;
        const contentType = req.query.contentType?.toString() || 'application/octet-stream';

        if (!filename || !expiry || !signature) {
            return res.status(400).send('Missing required parameters');
        }
        
        const payload = `${filename}:${expiry}:${contentType}`;
        const currentTime = Math.floor(Date.now() / 1000);

        if (!verifySignature(payload, signature.toString()) || currentTime > parseInt(expiry.toString(), 10)) {
            return res.status(403).send('Invalid or expired signature');
        }

        storageService.download(filename.toString())
            .then((data) => {
                res.setHeader('Content-Type', contentType);
                res.send(data);
            })
            .catch((error) => {
                res.status(500).send(`Failed to download file: ${error}`);
            });
    });

    router.get('/status', cors<express.Request>(corsConfig), (_req, res) => {
        res.send('Service is running');
    });

    return router;
}
