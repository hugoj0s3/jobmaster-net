namespace JobMaster.NatsJetStream.Background;

internal enum AckOutcome { None, Ack, Nak, Term }