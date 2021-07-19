//! A custom Quantity type that actually parses the `Quantity` type from k8s to something usable
//! with real numbers

// NOTE: This whole module might be useful to other people doing k8s things in Rust. We may want to
// eventually export this or create a separate crate if people want to reuse it

use k8s_openapi::apimachinery::pkg::api::resource::Quantity as KubeQuantity;

const KILOBYTE: f64 = 1000.0;
const MEGABYTE: f64 = 1_000_000.0; // 1000 ^ 2
const GIGABYTE: f64 = 1_000_000_000.0; // 1000 ^ 3
const TERABYTE: f64 = 1_000_000_000_000.0; // 1000 ^ 4
const PETABYTE: f64 = 1_000_000_000_000_000.0; // 1000 ^ 5
const EXABYTE: f64 = 1_000_000_000_000_000_000.0; // 1000 ^ 6
const KIBIBYTE: f64 = 1024.0;
const MEBIBYTE: f64 = 1_048_576.0; // 1024 ^ 2
const GIBIBYTE: f64 = 1_073_741_824.0; // 1024 ^ 3
const TEBIBYTE: f64 = 1_099_511_627_776.0; // 1024 ^ 4
const PEBIBYTE: f64 = 1_125_899_906_842_624.0; // 1024 ^ 5
const EXBIBYTE: f64 = 1_152_921_504_606_846_976.0; // 1024 ^ 6
const MILLICPU: f64 = 1.0 / 1000.0;

// Allows us to round to a precision of 3 for CPU values
const PRECISION_DIVISOR: f64 = 1000.0;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Suffix {
    Kilobyte,
    Megabyte,
    Gigabyte,
    Terabyte,
    Petabyte,
    Exabyte,
    Kibibyte,
    Mebibyte,
    Gibibyte,
    Tebibyte,
    Pebibyte,
    Exbibyte,
    Millicpu,
    None,
}

impl Suffix {
    /// Returns the multiplier needed to convert a number to the base value for a quantity with this
    /// suffix (bytes for memory and a float number for CPUs representing a possible fractional
    /// number of cores)
    pub(crate) const fn get_value(&self) -> f64 {
        match &self {
            Self::Kilobyte => KILOBYTE,
            Self::Megabyte => MEGABYTE,
            Self::Gigabyte => GIGABYTE,
            Self::Terabyte => TERABYTE,
            Self::Petabyte => PETABYTE,
            Self::Exabyte => EXABYTE,
            Self::Kibibyte => KIBIBYTE,
            Self::Mebibyte => MEBIBYTE,
            Self::Gibibyte => GIBIBYTE,
            Self::Tebibyte => TEBIBYTE,
            Self::Pebibyte => PEBIBYTE,
            Self::Exbibyte => EXBIBYTE,
            Self::Millicpu => MILLICPU,
            // If there is no units, just return a 1
            Self::None => 1.0,
        }
    }
}

impl AsRef<str> for Suffix {
    fn as_ref(&self) -> &str {
        match self {
            Self::Kilobyte => "K",
            Self::Megabyte => "M",
            Self::Gigabyte => "G",
            Self::Terabyte => "T",
            Self::Petabyte => "P",
            Self::Exabyte => "E",
            Self::Kibibyte => "Ki",
            Self::Mebibyte => "Mi",
            Self::Gibibyte => "Gi",
            Self::Tebibyte => "Ti",
            Self::Pebibyte => "Pi",
            Self::Exbibyte => "Ei",
            Self::Millicpu => "m",
            Self::None => "",
        }
    }
}

/// A wrapper around a normal Kubernetes quantity to indicate the type of resource this is.
///
/// Unfortunately, due to the quantity type being used for something that is entirely different
/// units, we cannot infer the type (memory or cpu) purely from the data involved. Specifically, for
/// a CPU measurement, it could be an integer value (such as "2") with no units attached. The lack
/// of a unit and the fact it isn't a float makes it impossible to assume it is a CPU measurement
/// instead of a number of memory bytes. Although it is highly unlikely that a memory quantity would
/// be as low as a max CPU count even on a big machine, we cannot just blindly assume here. So we
/// use this type to force the caller to explicitly state which kind of resource this is for
pub(crate) enum QuantityType<'a> {
    Memory(&'a KubeQuantity),
    Cpu(&'a KubeQuantity),
}

impl<'a> QuantityType<'a> {
    fn inner(&self) -> &'a KubeQuantity {
        match self {
            Self::Memory(v) => *v,
            Self::Cpu(v) => *v,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Quantity {
    /// This is the number of bytes of memory requested
    Memory(u128),
    /// The amount of CPU requested as a float, where 1.0 is one CPU core
    Cpu(f64),
}

impl Quantity {
    /// Attempts to convert a Kubernetes API quantity to our own `Quantity` type. The kubernetes
    /// quantity must be wrapped in `QuantityType`. See that type's documentation for more
    /// information on the reasoning behind this. This function will error if `QuantityType` does
    /// not match the type as parsed or if for some reason the underlying number value is malformed
    pub(crate) fn from_kube_quantity(q: QuantityType<'_>) -> anyhow::Result<Self> {
        let suffix = get_suffix(q.inner());
        // Brief validation
        if matches!(q, QuantityType::Cpu(_)) && !matches!(suffix, Suffix::Millicpu | Suffix::None) {
            anyhow::bail!("Attempted to parse a memory quantity as a cpu quantity")
        } else if matches!(q, QuantityType::Memory(_)) && matches!(suffix, Suffix::Millicpu) {
            anyhow::bail!("Attempted to parse a cpu quantity as a memory quantity")
        }

        // First get the raw value and strip off any suffixes and whitespace
        let raw = q.inner().0.trim().trim_end_matches(char::is_alphabetic);

        // Parse everything as a float, even though it could be an int. We'll convert at the end
        let parsed: f64 = raw.parse()?;

        // Now calculate the raw number of units
        let calculated = parsed * suffix.get_value();

        match q {
            QuantityType::Memory(_) => Ok(Quantity::Memory(calculated as u128)),
            // Round to 3 digits of CPU as 1m (or .001) is the lowest possible unit
            QuantityType::Cpu(_) => {
                let mut rounded = (calculated * PRECISION_DIVISOR).round() / PRECISION_DIVISOR;
                // If the number was even lower (e.g. .0000004), round up to the minimum
                if rounded < MILLICPU {
                    rounded = MILLICPU
                }
                Ok(Quantity::Cpu(rounded))
            }
        }
    }
}

/// A helper function for fetching the suffix from a quantity. This is useful for performing
/// conversions on a given quantity (as in the `divisor` for DownwardAPI volumes). This _does not_
/// perform any validations, simply returning `Suffix::None` in the case of a malformed quantity or
/// unrecognized suffix
pub(crate) fn get_suffix(q: &KubeQuantity) -> Suffix {
    let raw = q.0.as_str();
    // We don't need to validate here as we are just trying to get the suffix, so just check if it
    // is empty to avoid any out of bounds errors below
    if raw.is_empty() {
        return Suffix::None;
    }
    // get the suffix value by finding the first number value and taking everything after
    let raw_suffix = match raw.rsplit_once(char::is_numeric) {
        // If we never found a number, then this is technically malformed according to the API, but
        // we can at least check the rest of the string for a suffix
        None => raw,
        Some((_, suffix)) => suffix,
    };
    match raw_suffix {
        "m" => Suffix::Millicpu,
        "K" => Suffix::Kilobyte,
        "M" => Suffix::Megabyte,
        "G" => Suffix::Gigabyte,
        "T" => Suffix::Terabyte,
        "P" => Suffix::Petabyte,
        "E" => Suffix::Exabyte,
        "Ki" => Suffix::Kibibyte,
        "Mi" => Suffix::Mebibyte,
        "Gi" => Suffix::Gibibyte,
        "Ti" => Suffix::Tebibyte,
        "Pi" => Suffix::Pebibyte,
        "Ei" => Suffix::Exbibyte,
        _ => Suffix::None,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_suffixes() {
        let all_suffixes = vec![
            (KubeQuantity("500m".into()), Suffix::Millicpu),
            (KubeQuantity("123456".into()), Suffix::None),
            (KubeQuantity("1.23456".into()), Suffix::None),
            (KubeQuantity("500K".into()), Suffix::Kilobyte),
            (KubeQuantity("500M".into()), Suffix::Megabyte),
            (KubeQuantity("500G".into()), Suffix::Gigabyte),
            (KubeQuantity("500T".into()), Suffix::Terabyte),
            (KubeQuantity("500P".into()), Suffix::Petabyte),
            (KubeQuantity("500E".into()), Suffix::Exabyte),
            (KubeQuantity("500Ki".into()), Suffix::Kibibyte),
            (KubeQuantity("500Mi".into()), Suffix::Mebibyte),
            (KubeQuantity("500Gi".into()), Suffix::Gibibyte),
            (KubeQuantity("500Ti".into()), Suffix::Tebibyte),
            (KubeQuantity("500Pi".into()), Suffix::Pebibyte),
            (KubeQuantity("500Ei".into()), Suffix::Exbibyte),
        ];

        for (quantity, expected) in all_suffixes {
            let parsed = get_suffix(&quantity);
            assert_eq!(parsed, expected, "Expected parsed suffix to equal expected");
        }
    }

    #[test]
    fn test_invalid_suffixes() {
        let all_suffixes = vec![
            (KubeQuantity("".into()), "an empty", Suffix::None),
            (
                KubeQuantity("K".into()),
                "a missing number",
                Suffix::Kilobyte,
            ),
            (
                KubeQuantity("1.23456gigawatts".into()),
                "an invalid suffix",
                Suffix::None,
            ),
        ];

        for (quantity, description, expected) in all_suffixes {
            let parsed = get_suffix(&quantity);
            assert_eq!(
                parsed, expected,
                "Expected parsed suffix to equal expected for {} quantity",
                description
            );
        }
    }

    #[test]
    fn test_valid_memory_quantities() {
        let mem_quantities = vec![
            (KubeQuantity("123456".into()), Quantity::Memory(123456)),
            (KubeQuantity("123e8".into()), Quantity::Memory(12300000000)),
            (KubeQuantity("500K".into()), Quantity::Memory(500000)),
            (KubeQuantity("500M".into()), Quantity::Memory(500000000)),
            (KubeQuantity("500G".into()), Quantity::Memory(500000000000)),
            (
                KubeQuantity("500T".into()),
                Quantity::Memory(500000000000000),
            ),
            (
                KubeQuantity("500P".into()),
                Quantity::Memory(500000000000000000),
            ),
            (
                KubeQuantity("500E".into()),
                Quantity::Memory(500000000000000000000),
            ),
            (KubeQuantity("500Ki".into()), Quantity::Memory(512000)),
            (KubeQuantity("500Mi".into()), Quantity::Memory(524288000)),
            (KubeQuantity("500Gi".into()), Quantity::Memory(536870912000)),
            (
                KubeQuantity("500Ti".into()),
                Quantity::Memory(549755813888000),
            ),
            (
                KubeQuantity("500Pi".into()),
                Quantity::Memory(562949953421312000),
            ),
            (
                KubeQuantity("500Ei".into()),
                Quantity::Memory(576460752303423488000),
            ),
        ];

        for (q, expected) in mem_quantities {
            let quantity = Quantity::from_kube_quantity(QuantityType::Memory(&q))
                .expect("Conversion should not error");
            assert_eq!(
                quantity, expected,
                "Expected converted memory quantity {:?} to be equal",
                q
            )
        }
    }

    #[test]
    fn test_valid_cpu_quantities() {
        let mem_quantities = vec![
            (KubeQuantity("1".into()), Quantity::Cpu(1.0)),
            (KubeQuantity("1.25".into()), Quantity::Cpu(1.25)),
            // checks for rounding
            (KubeQuantity("1.2345".into()), Quantity::Cpu(1.235)),
            // makes sure that something less than the allowed quantity rounds up to the minimum
            (KubeQuantity(".000001".into()), Quantity::Cpu(0.001)),
            // check that .1m rounds to 1m
            (KubeQuantity(".1m".into()), Quantity::Cpu(0.001)),
            (KubeQuantity("1234m".into()), Quantity::Cpu(1.234)),
            (KubeQuantity("500m".into()), Quantity::Cpu(0.5)),
        ];

        for (q, expected) in mem_quantities {
            let quantity = Quantity::from_kube_quantity(QuantityType::Cpu(&q))
                .expect("Conversion should not error");
            assert_eq!(
                quantity, expected,
                "Expected converted cpu quantity {:?} to be equal",
                q
            )
        }
    }

    #[test]
    fn test_invalid_quantities() {
        Quantity::from_kube_quantity(QuantityType::Memory(&KubeQuantity("500m".into())))
            .expect_err("Should error when wrapping a CPU quantity with memory");

        Quantity::from_kube_quantity(QuantityType::Cpu(&KubeQuantity("500K".into())))
            .expect_err("Should error when wrapping a memory quantity with CPU");

        Quantity::from_kube_quantity(QuantityType::Memory(&KubeQuantity("TK421".into())))
            .expect_err("Should error with invalid number");
        Quantity::from_kube_quantity(QuantityType::Memory(&KubeQuantity("".into())))
            .expect_err("Should error with empty string");
    }
}
