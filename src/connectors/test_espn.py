"""
Test script for ESPN API connector

Uses the existing credentials from draft-wizard-adp to verify connectivity
"""

import logging
import sys
sys.path.append('.')

from src.connectors.espn_api import ESPNConnector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Test ESPN connector with existing credentials"""
    print("\n🏈 Testing ESPN API Connector 🏈")
    print("=" * 40)
    
    # Credentials from existing draft wizard
    LEAGUE_ID = 537814
    YEAR = 2025
    SWID = "3C010FF1-0860-485F-BF82-17FC9D702287"
    ESPN_S2 = "AEA3Vq4gY3g0nEBJbDSO8wc%2F0VEYfotMhZQRRIECUt63Hn7kpx84dQfh3Skfg2ZmfjnBt6Z8RDRL2H0g8rcthTl6acbEKLe%2FMGaaMYM63cjankHE%2B182LQjoKN787%2Fzfm%2BrQt2BoIAed2A0ooHsPqTLv197%2BRJH1opJSPCPDxlhwOkvcAXKtE8NWXTEmDDu6VMsT6UasOb7LWJYQEtRJoaVlEcEKAvaEVzkkcRHHga%2BjRh8q8QN82LCUp4UzhE2Zs1VUDKSTrzTwqy31YaACHfD%2BjnN%2BVvxwXq9Y3Ef2SzDLSnPOKKZWYnuFzY%2FJiODbCes%3D"
    
    # Test connection
    connector = ESPNConnector(
        league_id=LEAGUE_ID,
        year=YEAR, 
        swid=SWID,
        espn_s2=ESPN_S2
    )
    
    print("Testing connection...")
    if connector.test_connection():
        print("✅ Connection successful!")
        
        # Get league settings
        settings = connector.get_league_settings()
        if settings:
            print(f"\nLeague Details:")
            print(f"  Name: {settings.name}")
            print(f"  Season: {settings.season}")
            print(f"  Current Week: {settings.current_week}")
            print(f"  Teams: {settings.num_teams}")
            print(f"  Scoring: {settings.scoring_type}")
            print(f"  Superflex: {'Yes' if settings.is_superflex else 'No'}")
            
            print(f"\nRoster Configuration:")
            for position, count in settings.roster_slots.items():
                print(f"  {position}: {count}")
        
        # Test team data
        print("\nTesting team data...")
        teams = connector.get_teams()
        print(f"Retrieved {len(teams)} teams")
        
        if teams:
            print("First 3 teams:")
            for i, team in enumerate(teams[:3]):
                team_info = team.get("teamName", "Unknown")
                owner = team.get("primaryOwner", "Unknown")
                print(f"  {i+1}. {team_info} (Owner: {owner})")
                
    else:
        print("❌ Connection failed!")
        print("Check your credentials and network connection")

if __name__ == "__main__":
    main()