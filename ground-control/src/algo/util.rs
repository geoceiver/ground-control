



// Derived from on https://github.com/planet36/ecef-geodetic/blob/main/olson_1996/olson_1996.c
// Converts ECEF coordinates to geodetic coordinates (latitude, longitude, height)
// Based on the algorithm from: https://ieeexplore.ieee.org/document/481290

// # Arguments
// * `x` - X coordinate in meters
// * `y` - Y coordinate in meters
// * `z` - Z coordinate in meters

// # Returns
// A tuple of (latitude in radians, longitude in radians, height in meters)

pub fn ecef_to_latlon(x: f64, y: f64, z: f64) -> (f64, f64, f64) {
    // WGS-84 ellipsoid parameters
    let a = 6378137.0; // semi-major axis
    let e2 = 6.6943799901377997e-3; // first eccentricity squared
    let a1 = 4.2697672707157535e+4; // a*e2
    let a2 = 1.8230912546075455e+9; // a1*a1
    let a3 = 1.4291722289812413e+2; // a1*e2/2
    let a4 = 4.5577281365188637e+9; // (5/2)*a2
    let a5 = 4.2840589930055659e+4; // a1 + a3
    let a6 = 9.9330562000986220e-1; // 1 - e2

    let zp = z.abs();
    let w2 = x * x + y * y;
    let w = w2.sqrt();
    let z2 = z * z;
    let r2 = w2 + z2;
    let r = r2.sqrt();

    if r < 100000.0 {
        // Return invalid values for positions near Earth's center
        return (0.0, 0.0, -1.0e7);
    }

    let lon = y.atan2(x);
    let s2 = z2 / r2;
    let c2 = w2 / r2;

    let u = a2 / r;
    let v = a3 - a4 / r;

    let (s, c, ss) = if c2 > 0.3 {
        let s = (zp / r) * (1.0 + c2 * (a1 + u + s2 * v) / r);
        let ss = s * s;
        let c = (1.0 - ss).sqrt();
        (s, c, ss)
    } else {
        let c = (w / r) * (1.0 - s2 * (a5 - u - c2 * v) / r);
        let ss = 1.0 - c * c;
        let s = ss.sqrt();
        (s, c, ss)
    };

    let mut lat = if c2 > 0.3 { s.asin() } else { c.acos() };
    let g = 1.0 - e2 * ss;
    let rg = a / g.sqrt();
    let rf = a6 * rg;
    let u = w - rg * c;
    let v = zp - rf * s;
    let f = c * u + s * v;
    let m = c * v - s * u;
    let p = m / (rf / g + f);

    lat += p;
    let ht = f + m * p / 2.0;

    let lat = if z < 0.0 { -lat } else { lat };

    (lat, lon, ht)
}
