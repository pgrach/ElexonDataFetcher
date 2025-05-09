/**
 * Curtailment Routes
 * 
 * Defines API endpoints for curtailment-related data.
 */

import { Router } from 'express';
import * as curtailmentController from '../controllers/curtailmentController';

const router = Router();

// Daily curtailment endpoints
router.get('/daily/:date/farms', curtailmentController.getDailyCurtailmentByFarm);

// Monthly mining potential endpoints
router.get('/monthly-mining-potential/:month', curtailmentController.getMonthlyMiningPotential);

export default router;