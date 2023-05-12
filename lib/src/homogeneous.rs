//! Homogeneous processor module. This module uses Core struct.
use crate::processor::Processor;

pub struct HomogeneousProcessor {
    //Temporary warning avoidance. Remove this field when the processor is implemented.
    #[allow(dead_code)]
    processor: Processor,
}
