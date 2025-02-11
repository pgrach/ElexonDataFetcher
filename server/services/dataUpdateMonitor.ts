import { DifficultyAnomalyType, ProcessingAlert } from '../types/monitoring';
import { getUpdateServiceStatus } from './dataUpdater';
import OpenAI from 'openai';

const openai = new OpenAI({ baseURL: "https://api.x.ai/v1", apiKey: process.env.XAI_API_KEY });
const ALERT_THRESHOLD_MINUTES = 15;

async function analyzeUpdatePattern(updates: Array<{ timestamp: Date, success: boolean }>): Promise<string> {
  try {
    const response = await openai.chat.completions.create({
      model: "grok-2-1212",
      messages: [
        {
          role: "system",
          content: "Analyze the pattern of data updates and identify potential issues or anomalies."
        },
        {
          role: "user",
          content: JSON.stringify(updates)
        }
      ]
    });

    return response.choices[0].message.content;
  } catch (error) {
    console.error('Error analyzing update pattern:', error);
    return 'Unable to analyze update pattern';
  }
}

export async function checkUpdateHealth(): Promise<ProcessingAlert[]> {
  const alerts: ProcessingAlert[] = [];
  const status = getUpdateServiceStatus();
  const now = new Date();

  // Check for delayed updates
  if (status.lastSuccessfulUpdate) {
    const timeSinceUpdate = now.getTime() - status.lastSuccessfulUpdate.getTime();
    const minutesSinceUpdate = Math.floor(timeSinceUpdate / 60000);

    if (minutesSinceUpdate > ALERT_THRESHOLD_MINUTES) {
      alerts.push({
        type: 'UPDATE_DELAY',
        severity: 'HIGH',
        message: `No successful updates in the last ${minutesSinceUpdate} minutes`,
        timestamp: new Date()
      });
    }
  }

  // Check if update is stuck
  if (status.isCurrentlyUpdating && status.lastUpdateTime) {
    const updateDuration = now.getTime() - status.lastUpdateTime.getTime();
    if (updateDuration > 10 * 60 * 1000) { // 10 minutes
      alerts.push({
        type: 'UPDATE_STUCK',
        severity: 'CRITICAL',
        message: 'Update process appears to be stuck',
        timestamp: new Date()
      });
    }
  }

  return alerts;
}

export function startMonitoring(checkInterval: number = 5 * 60 * 1000): void {
  console.log('Starting data update monitoring service');
  
  setInterval(async () => {
    try {
      const alerts = await checkUpdateHealth();
      
      if (alerts.length > 0) {
        console.error('\n=== Data Update Alerts ===');
        alerts.forEach(alert => {
          console.error(`[${alert.severity}] ${alert.message}`);
        });
      }
    } catch (error) {
      console.error('Error in update monitoring:', error);
    }
  }, checkInterval);
}
