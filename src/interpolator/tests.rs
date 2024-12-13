use super::*;

#[test]
fn _interpolate_1d() {
    let interpolator = Interpolator {
        x_vals: vec![1., 2., 3., 4., 5.],
        y_vals: vec![2., 4., 6., 8., 10.],
    };
    let x = 2.5;
    let result = interpolator.interpolate(x).unwrap();
    let expected = (3., 5.);
    assert_eq!(expected, result);
}

#[test]
fn _interpolate_nd() {
    let interpolator = Interpolator {
        x_vals: vec![1., 2., 3., 4., 5.],
        y_vals: vec![vec![2., 4., 6., 8., 10.], vec![3., 5., 7., 9., 11.], vec![
            1., 2., 3., 4., 5.,
        ]],
    };
    let x = 2.5;
    let result = interpolator.interpolate(x).unwrap();
    let expected = (3., vec![2.0, 3.5, 5.0, 6.5, 8.0]);

    dbg!(&expected, &result);
    assert_eq!(expected, result);
}

#[test]
#[should_panic]
// TODO match on the specific error type.
fn _interpolate_1d_too_small() {
    let interpolator = Interpolator {
        x_vals: vec![1., 2., 3., 4., 5.],
        y_vals: vec![2., 4., 6., 8., 10.],
    };
    let x = 0.5;
    let _result = interpolator.interpolate(x).unwrap();
}

#[test]
#[should_panic]
// TODO match on the specific error type.
fn _interpolate_1d_too_big() {
    let interpolator = Interpolator {
        x_vals: vec![1., 2., 3., 4., 5.],
        y_vals: vec![2., 4., 6., 8., 10.],
    };
    let x = 6.;
    let _result = interpolator.interpolate(x).unwrap();
}

#[test]
#[should_panic]
// TODO match on the specific error type.
fn _interpolate_1d_nan() {
    let interpolator = Interpolator {
        x_vals: vec![1., 2., 3., 4., 5.],
        y_vals: vec![2., 4., 6., 8., 10.],
    };
    let x = f64::NAN;
    let _result = interpolator.interpolate(x).unwrap();
}
