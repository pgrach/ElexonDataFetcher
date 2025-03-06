/**
 * Checkpoint management for long-running processes
 * 
 * This module provides a consistent way to track progress and handle recovery
 * for long-running operations like reconciliation processes.
 */

import fs from 'fs';
import path from 'path';
import { logger } from './logger';

// Base directory for checkpoint files
const CHECKPOINT_DIR = './logs/checkpoints';

/**
 * Base interface for all checkpoint types
 */
export interface BaseCheckpoint {
  id: string;            // Unique identifier for this checkpoint
  created: string;       // Creation timestamp
  lastUpdated: string;   // Last update timestamp
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number;      // Progress percentage (0-100)
}

/**
 * Generic checkpoint manager for tracking progress of long-running operations
 */
export class CheckpointManager<T extends BaseCheckpoint> {
  private checkpointPath: string;
  private checkpointData: T | null = null;
  private autoSaveInterval: NodeJS.Timeout | null = null;
  
  /**
   * Create a new checkpoint manager
   * 
   * @param processName - Name of the process (used in file path)
   * @param initialData - Initial checkpoint data if starting fresh
   * @param autoSaveSeconds - How often to auto-save (0 to disable)
   */
  constructor(
    private processName: string,
    private initialData: T,
    private autoSaveSeconds: number = 5
  ) {
    // Ensure checkpoint directory exists
    if (!fs.existsSync(CHECKPOINT_DIR)) {
      fs.mkdirSync(CHECKPOINT_DIR, { recursive: true });
    }
    
    this.checkpointPath = path.join(CHECKPOINT_DIR, `${processName}.json`);
    
    // Set up auto-save if enabled
    if (this.autoSaveSeconds > 0) {
      this.startAutoSave();
    }
  }
  
  /**
   * Load checkpoint data
   */
  public load(): T | null {
    try {
      if (fs.existsSync(this.checkpointPath)) {
        const data = JSON.parse(fs.readFileSync(this.checkpointPath, 'utf8'));
        this.checkpointData = data;
        return data;
      }
    } catch (error) {
      logger.error(`Failed to load checkpoint for ${this.processName}`, {
        module: 'checkpoints',
        error: error as Error
      });
    }
    
    return null;
  }
  
  /**
   * Check if a checkpoint exists
   */
  public exists(): boolean {
    return fs.existsSync(this.checkpointPath);
  }
  
  /**
   * Initialize a new checkpoint or load existing
   */
  public init(): T {
    const existing = this.load();
    if (existing) {
      logger.info(`Loaded existing checkpoint for ${this.processName}`, {
        module: 'checkpoints',
        context: {
          id: existing.id,
          status: existing.status,
          progress: existing.progress
        }
      });
      return existing;
    }
    
    // Create new checkpoint
    this.checkpointData = {
      ...this.initialData,
      created: new Date().toISOString(),
      lastUpdated: new Date().toISOString()
    };
    
    // Save the initial checkpoint
    this.save();
    
    logger.info(`Created new checkpoint for ${this.processName}`, {
      module: 'checkpoints',
      context: {
        id: this.checkpointData.id,
        status: this.checkpointData.status,
      }
    });
    
    return this.checkpointData;
  }
  
  /**
   * Update checkpoint data
   */
  public update(updateFn: (data: T) => Partial<T>): T {
    if (!this.checkpointData) {
      throw new Error(`Checkpoint for ${this.processName} not initialized`);
    }
    
    // Apply updates
    const updates = updateFn(this.checkpointData);
    this.checkpointData = {
      ...this.checkpointData,
      ...updates,
      lastUpdated: new Date().toISOString()
    };
    
    // Save the updated checkpoint
    this.save();
    
    return this.checkpointData;
  }
  
  /**
   * Save checkpoint to disk
   */
  public save(): void {
    if (!this.checkpointData) {
      return;
    }
    
    try {
      fs.writeFileSync(
        this.checkpointPath,
        JSON.stringify(this.checkpointData, null, 2),
        'utf8'
      );
    } catch (error) {
      logger.error(`Failed to save checkpoint for ${this.processName}`, {
        module: 'checkpoints',
        error: error as Error
      });
    }
  }
  
  /**
   * Complete the checkpoint
   */
  public complete(success: boolean = true): void {
    if (!this.checkpointData) {
      return;
    }
    
    this.update(data => ({
      status: success ? 'completed' : 'failed',
      progress: success ? 100 : data.progress
    }));
    
    // Stop auto-save when completed
    this.stopAutoSave();
  }
  
  /**
   * Get current checkpoint data
   */
  public get(): T | null {
    return this.checkpointData;
  }
  
  /**
   * Clean up resources
   */
  public cleanup(): void {
    this.stopAutoSave();
  }
  
  /**
   * Delete the checkpoint file
   */
  public delete(): void {
    this.stopAutoSave();
    
    if (fs.existsSync(this.checkpointPath)) {
      fs.unlinkSync(this.checkpointPath);
    }
    
    this.checkpointData = null;
  }
  
  /**
   * Start auto-save timer
   */
  private startAutoSave(): void {
    this.stopAutoSave();
    
    this.autoSaveInterval = setInterval(() => {
      if (this.checkpointData) {
        this.save();
      }
    }, this.autoSaveSeconds * 1000);
  }
  
  /**
   * Stop auto-save timer
   */
  private stopAutoSave(): void {
    if (this.autoSaveInterval) {
      clearInterval(this.autoSaveInterval);
      this.autoSaveInterval = null;
    }
  }
}

/**
 * Reconciliation-specific checkpoint data
 */
export interface ReconciliationCheckpoint extends BaseCheckpoint {
  startDate: string;
  endDate: string;
  processedDates: string[];
  failedDates: Array<{ date: string; reason: string }>;
  currentDate: string | null;
  stats: {
    totalRecords: number;
    processedRecords: number;
    successfulRecords: number;
    failedRecords: number;
  };
}

/**
 * Create a checkpoint manager for reconciliation
 */
export function createReconciliationCheckpoint(
  processName: string,
  startDate: string,
  endDate: string
): CheckpointManager<ReconciliationCheckpoint> {
  return new CheckpointManager<ReconciliationCheckpoint>(
    processName,
    {
      id: `reconciliation_${Date.now()}`,
      created: new Date().toISOString(),
      lastUpdated: new Date().toISOString(),
      status: 'pending',
      progress: 0,
      startDate,
      endDate,
      processedDates: [],
      failedDates: [],
      currentDate: null,
      stats: {
        totalRecords: 0,
        processedRecords: 0,
        successfulRecords: 0,
        failedRecords: 0
      }
    }
  );
}

/**
 * Daily check checkpoint data
 */
export interface DailyCheckCheckpoint extends BaseCheckpoint {
  lastCheckDate: string;
  checkedDates: string[];
  processedDates: string[];
  stats: {
    recordsChecked: number;
    recordsFixed: number;
    datesMissingData: number;
  };
}

/**
 * Create a checkpoint manager for daily checks
 */
export function createDailyCheckCheckpoint(
  processName: string
): CheckpointManager<DailyCheckCheckpoint> {
  return new CheckpointManager<DailyCheckCheckpoint>(
    processName,
    {
      id: `daily_check_${Date.now()}`,
      created: new Date().toISOString(),
      lastUpdated: new Date().toISOString(),
      status: 'pending',
      progress: 0,
      lastCheckDate: new Date().toISOString(),
      checkedDates: [],
      processedDates: [],
      stats: {
        recordsChecked: 0,
        recordsFixed: 0,
        datesMissingData: 0
      }
    }
  );
}