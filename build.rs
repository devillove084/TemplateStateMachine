fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        // .type_attribute(".", "#[derive(Eq, PartialOrd, Hash)]")
        .type_attribute("consensus.ReplicaID", "#[derive(Eq, PartialOrd, Hash)]")
        .type_attribute("consensus.Seq", "#[derive(Eq, PartialOrd, Hash)]")
        .type_attribute(
            "consensus.LocalInstanceID",
            "#[derive(Eq, PartialOrd, Hash)]",
        )
        .type_attribute("consensus.Ballot", "#[derive(Eq, PartialOrd, Hash)]")
        .type_attribute("consensus.Command", "#[derive(Eq, Hash)]")
        .compile(&["proto/consensus.proto"], &["proto"])?;
    Ok(())
}
