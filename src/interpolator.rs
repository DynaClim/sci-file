// Simple 1-dimensional and n-dimensional linear interpolators for f64.

use serde::{Deserialize, Serialize};
use thiserror::Error;

//TODO add a "tolerance" and "value" cache.
// Using the interpolator will first check if the input is within the tolerance of the previously cached input
// And only interpolate if outside the tolerance.

/// Possible error conditions that may arise during interpolation.
#[derive(Debug, Error)]
pub enum InterpolationError {
    #[error("unable to interpolate value: {x} expected within range {x_min} and {x_max}")]
    OutOfBounds { x: f64, x_min: f64, x_max: f64 },
    #[error("attempted to interpolated NaN")]
    NaN,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct Interpolator<T>
where
    T: Default,
{
    pub(crate) x_vals: Vec<f64>,
    pub(crate) y_vals: Vec<T>,
}

impl<T: Clone + Default> Interpolator<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn init(&mut self, x_vals: &[f64], y_vals: &[T]) {
        self.x_vals = x_vals.to_vec();
        self.y_vals = y_vals.to_vec();
    }
}

impl Interpolator<f64> {
    // 1-D Interpolator.
    /// Provides the interpolated value
    /// # Errors
    ///
    /// `OutOfBounds` and `NaN`
    pub fn interpolate(&self, x: f64) -> Result<(f64, f64), InterpolationError> {
        sanity_check(x, &self.x_vals)?;
        Ok(interpolate_1d(x, &self.x_vals, &self.y_vals))
    }
}

impl Interpolator<Vec<f64>> {
    // n-D Interpolator.
    /// Provides the interpolated value, or an error if out of range.
    /// # Errors
    ///
    /// `OutOfBounds` and `NaN`
    pub fn interpolate(&self, x: f64) -> Result<(f64, Vec<f64>), InterpolationError> {
        sanity_check(x, &self.x_vals)?;
        Ok(interpolate(x, &self.x_vals, &self.y_vals))
    }
}

fn sanity_check(x: f64, x_vals: &[f64]) -> Result<(), InterpolationError> {
    if x.is_nan() {
        return Err(InterpolationError::NaN);
    }

    if x < x_vals[0] || x > x_vals[x_vals.len() - 1] {
        return Err(InterpolationError::OutOfBounds {
            x,
            x_min: x_vals[0],
            x_max: x_vals[x_vals.len() - 1],
        });
    }
    Ok(())
}

// Interpolation for 1-dimensional array.
fn interpolate_1d(x: f64, x_vals: &[f64], y_vals: &[f64]) -> (f64, f64) {
    match x_vals.binary_search_by(|val| val.total_cmp(&x)) {
        Ok(i) => {
            // Exact match found: x_vals[i] == x
            (x_vals[i], y_vals[i])
        }
        Err(i) => {
            // x_vals[i - 1] < x < x_vals[i]
            let prev_x = x_vals[i - 1];
            let next_x = x_vals[i];
            let delta = (x - prev_x) / (next_x - prev_x);
            let y = (1. - delta) * y_vals[i - 1] + delta * y_vals[i];

            (next_x, y)
        }
    }
}

// Interpolation for n-dimensional array.
fn interpolate(x: f64, x_vals: &[f64], y_vals: &[Vec<f64>]) -> (f64, Vec<f64>) {
    match x_vals.binary_search_by(|val| val.total_cmp(&x)) {
        Ok(i) => {
            // Exact match found: x_vals[i] == x
            (x_vals[i], y_vals[i].clone())
        }
        Err(i) => {
            // x_vals[i - 1] < x < x_vals[i]
            let prev_x = x_vals[i - 1];
            let next_x = x_vals[i];
            let delta = (x - prev_x) / (next_x - prev_x);
            let prev_y = &y_vals[i - 1];
            let next_y = &y_vals[i];
            let y = prev_y
                .iter()
                .zip(next_y.iter())
                .map(|(prev, next)| (1. - delta) * prev + delta * next)
                .collect();

            (next_x, y)
        }
    }
}

#[cfg(test)]
mod tests;
