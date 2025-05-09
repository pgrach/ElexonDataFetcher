/**
 * Summary Routes
 * 
 * Defines API endpoints for fetching summary data.
 */

import { Router } from 'express';
import * as summaryController from '../controllers/summaryController';

const router = Router();

// Lead party endpoints
router.get('/lead-parties', summaryController.getLeadParties);
router.get('/lead-parties/:date', summaryController.getCurtailedLeadParties);

// Summary endpoints
router.get('/daily/:date', summaryController.getDailySummary);
router.get('/monthly/:month', summaryController.getMonthlySummary);
router.get('/yearly/:year', summaryController.getYearlySummary);

export default router;