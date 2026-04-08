package query

func (s Service) DiscoveryTrustMetadata() TrustModeMetadata {
	return trustModeMetadataFromValue(s.discoveryTrustMode)
}

func (s Service) SearchTrustMetadata() TrustModeMetadata {
	return trustModeMetadataFromValue(s.searchTrustMode)
}

func trustModeMetadataFromValue(mode string) TrustModeMetadata {
	switch mode {
	case trustModePreferTrusted:
		return TrustModeMetadata{
			TrustMode:    trustModePreferTrusted,
			TrustApplied: true,
			ResultScope:  trustModePreferTrusted,
		}
	case trustModeTrustedOnly:
		return TrustModeMetadata{
			TrustMode:    trustModeTrustedOnly,
			TrustApplied: true,
			ResultScope:  trustModeTrustedOnly,
		}
	default:
		return TrustModeMetadata{
			TrustMode:    trustModeOpen,
			TrustApplied: false,
			ResultScope:  trustModeOpen,
		}
	}
}
